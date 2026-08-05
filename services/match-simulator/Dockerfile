FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY services/common ./common
COPY services/match-simulator/go.mod services/match-simulator/go.sum ./match-simulator/
WORKDIR /app/match-simulator
RUN go mod download
COPY services/match-simulator/. .
RUN CGO_ENABLED=0 GOOS=linux go build -o match-simulator .

FROM alpine:3.24
RUN apk --no-cache add ca-certificates && \
    adduser -D -u 10001 app
WORKDIR /app
COPY --from=builder /app/match-simulator/match-simulator .
USER app
CMD ["./match-simulator"]
