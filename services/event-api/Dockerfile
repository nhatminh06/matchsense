FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY services/common ./common
COPY services/event-api/go.mod services/event-api/go.sum ./event-api/
WORKDIR /app/event-api
RUN go mod download
COPY services/event-api/. .
RUN CGO_ENABLED=0 GOOS=linux go build -o event-api .

FROM alpine:3.24
RUN apk --no-cache add ca-certificates && \
    adduser -D -u 10001 app
WORKDIR /app
COPY --from=builder /app/event-api/event-api .
USER app
EXPOSE 8080
CMD ["./event-api"]
