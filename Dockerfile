# syntax=docker/dockerfile:1.7
FROM golang:1.26.2-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN go test ./...
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/earthquake-service ./cmd/earthquake-service
RUN mkdir -p /out/provider-state

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/earthquake-service /earthquake-service
COPY --from=build --chown=nonroot:nonroot /out/provider-state /var/lib/shaker/provider-state
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/earthquake-service"]
CMD ["--config", "/etc/shaker/config.toml", "all"]
