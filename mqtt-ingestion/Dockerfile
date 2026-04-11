# MQTT Ingestion Service Dockerfile

FROM node:20-alpine

WORKDIR /app

# Copy shared package first (for npm workspaces)
COPY packages/shared ./packages/shared

# Copy mqtt-ingestion
COPY mqtt-ingestion ./mqtt-ingestion

# Copy package files for workspace setup
COPY package.json package-lock.json ./

# Install dependencies
RUN npm ci --workspaces

# Build shared package
RUN cd packages/shared && npm run build

# Build mqtt-ingestion service
RUN cd mqtt-ingestion && npm run build

# Expose health check (optional)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD node -e "console.log('ok')" || exit 1

# Start service
CMD ["node", "mqtt-ingestion/dist/index.js"]
