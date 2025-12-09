# Use pre-built binary from goreleaser
FROM alpine:3.19

# Install ca-certificates for HTTPS connections
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN adduser -D -g '' appuser

# Copy pre-built binary from build context (dockers_v2 places binaries in platform dirs)
COPY linux/amd64/scyllamigrate /usr/local/bin/scyllamigrate

# Use non-root user
USER appuser

# Set default working directory for migrations
WORKDIR /migrations

ENTRYPOINT ["scyllamigrate"]
CMD ["-help"]
