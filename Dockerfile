# syntax=docker/dockerfile:1.7
FROM node:22-alpine AS frontend
WORKDIR /web
COPY web/package.json web/package-lock.json* ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM golang:1.26.2-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
# Embed the freshly built SPA (overwrites the .gitkeep placeholder).
COPY --from=frontend /internal/httpapi/web/dist ./internal/httpapi/web/dist
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/earthquake-service ./cmd/earthquake-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/earthquake-service /earthquake-service
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/earthquake-service"]
CMD ["all"]
