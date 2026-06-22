FROM node:20-alpine AS builder
WORKDIR /app

# Install all deps (including dev) needed for TypeScript + Vite build.
COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build:frontend
RUN npm run build:backend

FROM node:20-alpine AS runtime
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# Backend JS output + frontend assets
COPY --from=builder /app/dist ./dist

EXPOSE 3000

CMD ["node", "dist/index.js"]
