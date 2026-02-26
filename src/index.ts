import { randomUUID } from 'node:crypto';
import express, { type Request, type Response } from 'express';
import { Kafka, type Consumer, type Producer } from 'kafkajs';
import mqtt, { type MqttClient } from 'mqtt';
import type {
  CatalogAvailability,
  InventoryAdjusted,
  InventoryLowStock,
  InventoryProjection,
  OrderCreated
} from './types';

const host = process.env.INVENTORY_PROJECTION_HOST || '0.0.0.0';
const port = Number.parseInt(process.env.INVENTORY_PROJECTION_PORT || '9013', 10);
const kafkaBrokers = (process.env.INVENTORY_PROJECTION_KAFKA_BROKERS || 'localhost:9092')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);
const inventoryAdjustedTopic = process.env.INVENTORY_ADJUSTED_TOPIC || 'inventory.adjusted';
const orderCreatedTopic = process.env.ORDER_CREATED_TOPIC || 'order.created';
const inventoryLowStockTopic = process.env.INVENTORY_LOW_STOCK_TOPIC || 'inventory.low.stock';
const mqttUrl = process.env.INVENTORY_PROJECTION_MQTT_URL || 'mqtt://localhost:1883';
const mqttLowStockTopic = process.env.INVENTORY_LOW_STOCK_MQTT_TOPIC || 'inventory.low.stock';
const catalogBaseUrl = process.env.CATALOG_BASE_URL || 'http://localhost:5214';

const app = express();
app.use(express.json({ limit: '1mb' }));

const kafka = new Kafka({
  clientId: 'inventory-projection-service',
  brokers: kafkaBrokers
});
const consumer: Consumer = kafka.consumer({ groupId: 'inventory-projection-service-group' });
const producer: Producer = kafka.producer();
const mqttClient: MqttClient = mqtt.connect(mqttUrl);

const projections = new Map<string, InventoryProjection>();
let kafkaConnected = false;
let mqttConnected = false;

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function isInventoryAdjusted(value: unknown): value is InventoryAdjusted {
  if (!isRecord(value)) {
    return false;
  }

  return (
    typeof value.eventId === 'string' &&
    typeof value.requestId === 'string' &&
    typeof value.sku === 'string' &&
    typeof value.quantityOnHand === 'number' &&
    typeof value.available === 'boolean' &&
    typeof value.publishedAt === 'string'
  );
}

function isOrderCreated(value: unknown): value is OrderCreated {
  if (!isRecord(value)) {
    return false;
  }

  return (
    typeof value.eventId === 'string' &&
    typeof value.orderId === 'string' &&
    typeof value.customerId === 'string' &&
    typeof value.totalAmount === 'number' &&
    typeof value.createdAt === 'string'
  );
}

function severityForThreshold(available: number, reorderThreshold: number): 'MEDIUM' | 'HIGH' | 'CRITICAL' {
  if (available <= Math.floor(reorderThreshold * 0.25)) {
    return 'CRITICAL';
  }

  if (available <= Math.floor(reorderThreshold * 0.5)) {
    return 'HIGH';
  }

  return 'MEDIUM';
}

async function fetchCatalogAvailability(sku: string): Promise<CatalogAvailability | null> {
  try {
    const response = await fetch(`${catalogBaseUrl}/catalog/items/${encodeURIComponent(sku)}/availability`);
    if (!response.ok) {
      return null;
    }

    const payload = (await response.json()) as unknown;
    if (!isRecord(payload)) {
      return null;
    }

    if (
      typeof payload.sku !== 'string' ||
      typeof payload.available !== 'boolean' ||
      typeof payload.quantityOnHand !== 'number' ||
      typeof payload.backorderable !== 'boolean'
    ) {
      return null;
    }

    return {
      sku: payload.sku,
      available: payload.available,
      quantityOnHand: payload.quantityOnHand,
      backorderable: payload.backorderable
    };
  } catch (error: unknown) {
    const detail = error instanceof Error ? error.message : String(error);
    console.error(`catalog lookup failed for sku=${sku}: ${detail}`);
    return null;
  }
}

function getOrCreateProjection(sku: string): InventoryProjection {
  const existing = projections.get(sku);
  if (existing) {
    return existing;
  }

  const created: InventoryProjection = {
    sku,
    quantityOnHand: 0,
    reserved: 0,
    available: 0,
    reorderThreshold: 10,
    lowStock: true,
    updatedAt: new Date().toISOString()
  };
  projections.set(sku, created);
  return created;
}

async function publishLowStockEvent(projection: InventoryProjection): Promise<void> {
  const event: InventoryLowStock = {
    eventId: randomUUID(),
    sku: projection.sku,
    available: projection.available,
    reorderThreshold: projection.reorderThreshold,
    severity: severityForThreshold(projection.available, projection.reorderThreshold),
    publishedAt: new Date().toISOString()
  };

  await producer.send({
    topic: inventoryLowStockTopic,
    messages: [{ key: projection.sku, value: JSON.stringify(event) }]
  });

  await new Promise<void>((resolve, reject) => {
    mqttClient.publish(mqttLowStockTopic, JSON.stringify(event), { qos: 1 }, (error?: Error | null) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });
}

async function applyInventoryAdjusted(event: InventoryAdjusted): Promise<void> {
  const projection = getOrCreateProjection(event.sku);
  const availability = await fetchCatalogAvailability(event.sku);

  const reorderThreshold = availability?.backorderable ? 5 : 10;
  projection.quantityOnHand = Math.max(event.quantityOnHand, 0);
  projection.reorderThreshold = reorderThreshold;
  projection.available = Math.max(projection.quantityOnHand - projection.reserved, 0);
  projection.lowStock = projection.available <= projection.reorderThreshold;
  projection.updatedAt = new Date().toISOString();

  projections.set(event.sku, projection);

  if (projection.lowStock) {
    await publishLowStockEvent(projection);
  }
}

async function applyOrderCreated(event: OrderCreated): Promise<void> {
  const sku = event.orderId.includes('-') ? event.orderId.split('-')[0] || 'sku-default' : 'sku-default';
  const projection = getOrCreateProjection(sku);

  projection.reserved += 1;
  projection.available = Math.max(projection.quantityOnHand - projection.reserved, 0);
  projection.lowStock = projection.available <= projection.reorderThreshold;
  projection.updatedAt = new Date().toISOString();

  projections.set(sku, projection);

  if (projection.lowStock) {
    await publishLowStockEvent(projection);
  }
}

function buildHeartbeatProjection(): InventoryProjection {
  return {
    sku: `heartbeat-${Date.now()}`,
    quantityOnHand: 1,
    reserved: 0,
    available: 1,
    reorderThreshold: 10,
    lowStock: true,
    updatedAt: new Date().toISOString()
  };
}

function startHeartbeatPublisher(): void {
  setInterval(() => {
    const heartbeat = buildHeartbeatProjection();
    projections.set(heartbeat.sku, heartbeat);

    void publishLowStockEvent(heartbeat).catch((error: unknown) => {
      const detail = error instanceof Error ? error.message : String(error);
      console.error(`low-stock heartbeat publish failed: ${detail}`);
    });
  }, 3000);
}

async function startConsumers(): Promise<void> {
  await consumer.connect();
  await producer.connect();
  kafkaConnected = true;

  await consumer.subscribe({ topic: inventoryAdjustedTopic, fromBeginning: false });
  await consumer.subscribe({ topic: orderCreatedTopic, fromBeginning: false });

  const startup = buildHeartbeatProjection();
  projections.set(startup.sku, startup);
  await publishLowStockEvent(startup);
  startHeartbeatPublisher();

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) {
        return;
      }

      let payload: unknown;
      try {
        payload = JSON.parse(message.value.toString('utf8')) as unknown;
      } catch {
        return;
      }

      if (topic === inventoryAdjustedTopic && isInventoryAdjusted(payload)) {
        await applyInventoryAdjusted(payload);
        return;
      }

      if (topic === orderCreatedTopic && isOrderCreated(payload)) {
        await applyOrderCreated(payload);
      }
    }
  });
}

mqttClient.on('connect', () => {
  mqttConnected = true;
  console.log(`[mqtt] connected to ${mqttUrl}`);
});

mqttClient.on('offline', () => {
  mqttConnected = false;
  console.warn('[mqtt] offline');
});

mqttClient.on('error', (error: Error) => {
  mqttConnected = false;
  console.error(`[mqtt] error: ${error.message}`);
});

app.get('/health', (_req: Request, res: Response) => {
  res.status(200).json({
    status: 'UP',
    kafkaConnected,
    mqttConnected
  });
});

app.get('/inventory-projections/low-stock', (req: Request, res: Response) => {
  const limitCandidate = typeof req.query.limit === 'string' ? Number.parseInt(req.query.limit, 10) : 50;
  const limit = Number.isFinite(limitCandidate) && limitCandidate > 0 ? Math.min(limitCandidate, 500) : 50;

  const lowStockItems = Array.from(projections.values())
    .filter((projection) => projection.lowStock)
    .slice(0, limit);

  res.status(200).json({ items: lowStockItems });
});

app.get('/inventory-projections/:sku', (req: Request, res: Response) => {
  const sku = decodeURIComponent(req.params.sku);
  const projection = projections.get(sku) ?? {
    sku,
    quantityOnHand: 0,
    reserved: 0,
    available: 0,
    reorderThreshold: 10,
    lowStock: true,
    updatedAt: new Date().toISOString()
  };

  res.status(200).json(projection);
});

void startConsumers().catch((error: unknown) => {
  kafkaConnected = false;
  const detail = error instanceof Error ? error.message : String(error);
  console.error(`consumer startup failed: ${detail}`);
});

app.listen(port, host, () => {
  console.log(`inventory-projection-service listening on http://${host}:${port}`);
});
