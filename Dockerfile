FROM golang:1.24.4 AS builder
ENV SRC=github.com/segmentio/data-digger
ENV CGO_ENABLED=0

COPY . /go/src/${SRC}
RUN cd /go/src/${SRC} && make install

FROM scratch

COPY --from=builder /go/bin/digger /bin/digger
ENTRYPOINT ["/bin/digger"]
