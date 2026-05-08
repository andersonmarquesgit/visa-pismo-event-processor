FROM golang:1.26-alpine AS build

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/processor ./cmd/processor

FROM alpine:3.20
WORKDIR /app
COPY --from=build /out/processor ./processor

CMD ["./processor"]
