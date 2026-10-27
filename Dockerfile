FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
COPY internal/ ./internal/
COPY cmd/dino/ ./cmd/dino/
COPY web/embed.go ./web/embed.go
COPY web/dist/ ./web/dist/
COPY packs/*.yaml /src-packs/
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/dino ./cmd/dino \
    && mkdir -p /image-data/inbox /image-data/output /image-packs \
    && cp /src-packs/*.yaml /image-packs/

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /bin/dino /bin/dino
COPY --from=builder --chown=10001:10001 /image-packs/ /deinopis/packs/
COPY schemas/ /deinopis/schemas/
COPY --chown=10001:10001 sources/ /deinopis/sources/
COPY --chown=10001:10001 sinks/ /deinopis/sinks/
COPY --from=builder --chown=10001:10001 /image-data/ /data/
USER 10001:10001
WORKDIR /deinopis
EXPOSE 1514/udp 1514/tcp 7514/tcp 8080/tcp 9090/tcp
VOLUME ["/data"]
ENTRYPOINT ["/bin/dino"]
CMD ["-syslog-udp", ":1514", "-syslog-tcp", ":1514", "-syslog-tls", "none", "-http", ":8080", "-ui", ":9090", "-inbox", "/data/inbox", "-output-file", "/data/output/normalized.ndjson", "-packs-dir", "/deinopis/packs", "-sources-dir", "/deinopis/sources", "-sinks-dir", "/deinopis/sinks", "-db", "/data/deinopis.db"]
