FROM alpine:3.20.3 AS builder

RUN apk update && apk add --no-cache ca-certificates

FROM scratch

WORKDIR /bin

COPY bin/news news

ENTRYPOINT ["news"]