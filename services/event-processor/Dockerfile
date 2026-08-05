FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY services/common ./common
COPY services/event-processor/go.mod services/event-processor/go.sum ./event-processor/
WORKDIR /app/event-processor
RUN go mod download
COPY services/event-processor/. .
RUN CGO_ENABLED=0 GOOS=linux go build -o event-processor .

FROM alpine:3.24
RUN apk --no-cache add ca-certificates && \
    adduser -D -u 10001 app
WORKDIR /app
COPY --from=builder /app/event-processor/event-processor .
USER app
CMD ["./event-processor"]
