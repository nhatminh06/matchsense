FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY services/common ./common
COPY services/query-api/go.mod services/query-api/go.sum ./query-api/
WORKDIR /app/query-api
RUN go mod download
COPY services/query-api/. .
RUN CGO_ENABLED=0 GOOS=linux go build -o query-api .

FROM alpine:3.20
RUN apk --no-cache add ca-certificates && \
    adduser -D -u 10001 app
WORKDIR /app
COPY --from=builder /app/query-api/query-api .
USER app
EXPOSE 8083
CMD ["./query-api"]
