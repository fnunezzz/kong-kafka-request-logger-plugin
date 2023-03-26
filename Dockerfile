FROM golang:1.20-alpine AS kong-request-logger-plugin-build-stage

RUN apk add --no-cache git gcc libc-dev protobuf-dev

WORKDIR /usr/local/build

COPY . .

RUN go mod tidy
RUN go build .