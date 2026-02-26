export interface InventoryProjection {
  sku: string;
  quantityOnHand: number;
  reserved: number;
  available: number;
  reorderThreshold: number;
  lowStock: boolean;
  updatedAt: string;
}

export interface InventoryAdjusted {
  eventId: string;
  requestId: string;
  sku: string;
  quantityOnHand: number;
  available: boolean;
  publishedAt: string;
}

export interface OrderCreated {
  eventId: string;
  orderId: string;
  customerId: string;
  totalAmount: number;
  createdAt: string;
}

export interface InventoryLowStock {
  eventId: string;
  sku: string;
  available: number;
  reorderThreshold: number;
  severity: 'MEDIUM' | 'HIGH' | 'CRITICAL';
  publishedAt: string;
}

export interface CatalogAvailability {
  sku: string;
  available: boolean;
  quantityOnHand: number;
  backorderable: boolean;
}
