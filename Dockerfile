FROM alpine:latest
RUN apk add --no-cache --progress git make go bash
ADD . /home/connect
RUN cd /home/connect && go build