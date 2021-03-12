FROM golang:1.15.10-alpine3.13
RUN apk add --update bash curl git && rm /var/cache/apk/*

WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .
WORKDIR cmd/smoketest
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /usr/local/bin/smoketest

WORKDIR ../casload
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /usr/local/bin/casload
