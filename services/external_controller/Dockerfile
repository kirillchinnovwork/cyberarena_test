FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum* ./
COPY ../../api ../api
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /controller ./cmd/controller

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /controller /controller
ENTRYPOINT ["/controller"]
