FROM golang:1.24.2 as builder
ENV SRC github.com/segmentio/data-digger
ENV CGO_ENABLED=0

COPY . /go/src/${SRC}
RUN cd /go/src/${SRC} && make install

FROM scratch

COPY --from=builder /go/bin/digger /bin/digger
ENTRYPOINT ["/bin/digger"]
