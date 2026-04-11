# Dashboard Server Dockerfile

FROM node:20-alpine

WORKDIR /app

# Copy shared package first (for npm workspaces)
COPY packages/shared ./packages/shared

# Copy dashboard-server
COPY dashboard-server ./dashboard-server

# Copy package files for workspace setup
COPY package.json package-lock.json ./

# Install dependencies
RUN npm ci --workspaces

# Build shared package
RUN cd packages/shared && npm run build

# Build dashboard server
RUN cd dashboard-server && npm run build

# Expose port
EXPOSE 3000

# Start service
CMD ["node", "dashboard-server/dist/index.js"]
