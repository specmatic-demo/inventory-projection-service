FROM node:24

WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY src ./src
RUN git clone https://github.com/specmatic-demo/central-contract-repository /app/.specmatic/repos/central-contract-repository

ENV INVENTORY_PROJECTION_HOST=0.0.0.0
ENV INVENTORY_PROJECTION_PORT=9013

EXPOSE 9013
CMD ["npm", "run", "start"]
