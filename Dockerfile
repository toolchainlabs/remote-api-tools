FROM golang:1.15.10-alpine3.13 AS builder
ARG APP_NAME
RUN apk add --update bash curl git && rm /var/cache/apk/*

WORKDIR /build

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .
WORKDIR cmd/${APP_NAME}
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ../../app

FROM alpine:3.13.2
WORKDIR /root/
COPY --from=builder /build/app /usr/local/bin/

ENTRYPOINT ["app"] 
CMD ["--help"]