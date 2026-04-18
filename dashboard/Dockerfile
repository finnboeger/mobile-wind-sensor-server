# Multi-stage build: frontend (build stage) + backend (runtime)

# Stage 1: Build Vue frontend
FROM node:20-alpine AS frontend-builder
WORKDIR /app

COPY package.json ./
RUN npm ci

COPY . .
RUN npm run build:frontend

# Stage 2: Build and run backend
FROM node:20-alpine

WORKDIR /app

COPY package.json ./
RUN npm ci --only=production

COPY src/backend ./src/backend
COPY tsconfig.json .

RUN npm run build:backend

# Copy built frontend from stage 1
COPY --from=frontend-builder /app/dist/public ./dist/public

EXPOSE 3000

CMD ["node", "dist/index.js"]
