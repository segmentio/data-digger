FROM golang:1.14 as builder
ENV SRC github.com/segmentio/data-digger
ENV CGO_ENABLED=1

COPY . /go/src/${SRC}
RUN cd /go/src/${SRC} && make install

FROM scratch

COPY --from=builder /go/bin/digger /bin/digger
ENTRYPOINT ["/bin/digger"]
