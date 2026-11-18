FROM golang:1.27.4-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /api ./cmd/api && CGO_ENABLED=0 go build -o /worker ./cmd/worker
FROM alpine:3.23
RUN adduser -D -u 10001 app && mkdir /data && chown app /data
COPY --from=build /api /worker /usr/local/bin/
USER app
WORKDIR /data
ENV PAYMENT_DB=/data/payments.db
CMD ["api"]
