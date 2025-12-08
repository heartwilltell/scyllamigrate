# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /scyllamigrate ./cmd

# Final stage
FROM alpine:3.19

# Install ca-certificates for HTTPS connections
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN adduser -D -g '' appuser

# Copy binary from builder
COPY --from=builder /scyllamigrate /usr/local/bin/scyllamigrate

# Use non-root user
USER appuser

# Set default working directory for migrations
WORKDIR /migrations

ENTRYPOINT ["scyllamigrate"]
CMD ["--help"]
