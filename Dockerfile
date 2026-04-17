# Multi-stage build: frontend (build stage) + backend (runtime)

# Stage 1: Build Vue frontend
FROM node:20-alpine as frontend-builder
WORKDIR /app

COPY dashboard/package.json ./
RUN npm ci

COPY dashboard .
RUN npm run build:frontend

# Stage 2: Build and run backend
FROM node:20-alpine

WORKDIR /app

# Copy backend dependencies
COPY dashboard/package.json ./
RUN npm ci --only=production

# Copy TypeScript source
COPY dashboard/src/backend ./src/backend
COPY dashboard/tsconfig.json .

# Compile TypeScript
RUN npm run build:backend

# Copy built frontend from stage 1
COPY --from=frontend-builder /app/dist/public ./dist/public

# Expose port
EXPOSE 3000

# Start server
CMD ["node", "dist/index.js"]
