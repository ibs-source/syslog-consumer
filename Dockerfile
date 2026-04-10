FROM golang:1.25.8-alpine AS build-stage

RUN apk update && \
    apk upgrade --no-cache && \
    apk add --no-cache git

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# PGO: default.pgo is committed in the repo root for ~5-15% throughput gain.
# Override with: docker build --build-arg PGO_PROFILE=other.pgo .
ARG PGO_PROFILE="default.pgo"

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOEXPERIMENT=greenteagc \
    go build -pgo=${PGO_PROFILE} -trimpath -ldflags="-s -w" -o syslog-consumer ./cmd/consumer

FROM amd64/alpine:3.23.3

ENV CERTIFICATE_PATH=/home/app/certificate
ENV CERTIFICATE_RENEWAL=2
ENV CERTIFICATE_DEPLOYER=https://TENANT.syslog.ibs.cloud/generate
ENV CERTIFICATE_CERTIFICATE_DEPLOYER_KEY=pWJ5MQe2xGtkzukPJykXh9ypEVuWMpv9JDsgeyFT5DExzqmAQwrguCGsg86AgdKk88uytjMaLP8dUrzJXxJ3vxirbRMSj8bu9ChfTmgBgpn6k2eAoDUySigXpDtmkT7f
ENV BROKER=ssl://syslog.ibs.cloud:8883

# Certificate renewal interval in seconds (default: 6 hours = 21600)
ENV CERTIFICATE_RENEWAL_INTERVAL=21600

# GC tuning: higher GOGC reduces GC frequency at the cost of more memory.
# With greenteagc, the GC is already more efficient. GOMEMLIMIT prevents OOM
# by triggering GC more aggressively as memory approaches the limit.
# Override GOMEMLIMIT at deployment based on container memory (target ~80%).
ENV GOGC=200
ENV GOMEMLIMIT=4GiB

ARG TIMEZONE="UTC"

RUN apk update && \
    apk upgrade --no-cache && \
    apk add --no-cache \
      bash \
      openssl \
      curl \
      wget \
      ca-certificates \
      iputils \
      tzdata && \
    rm -rf /var/cache/apk/*

# Create non-root user and required directories

RUN addgroup -S app && adduser -S -G app app && \
    mkdir -p ${CERTIFICATE_PATH} && \
    cp -r /usr/share/zoneinfo/${TIMEZONE} /etc/localtime && \
    echo "${TIMEZONE}" > /etc/timezone

# Copy the Go binary

COPY --from=build-stage /app/syslog-consumer /usr/local/bin/syslog-consumer

# Copy infrastructure scripts

COPY wrapper /usr/sbin/wrapper
COPY manager ${CERTIFICATE_PATH}/manager
COPY healthcheck /usr/sbin/healthcheck

# Set permissions

RUN chmod +x /usr/sbin/wrapper /usr/sbin/healthcheck /usr/local/bin/syslog-consumer ${CERTIFICATE_PATH}/manager && \
    chown -R app:app ${CERTIFICATE_PATH} && \
    chown root:root /usr/sbin/wrapper /usr/sbin/healthcheck /usr/local/bin/syslog-consumer ${CERTIFICATE_PATH}/manager

USER app

EXPOSE 9980/tcp

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD /usr/sbin/healthcheck

ENTRYPOINT ["/usr/sbin/wrapper"]
