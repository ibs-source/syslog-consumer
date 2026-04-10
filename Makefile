.PHONY: build build-pgo test test-race test-cover lint vet clean docker-build pgo

BINARY      := syslog-consumer
PKG         := ./cmd/consumer
PGO_PROFILE := default.pgo

build:
	CGO_ENABLED=0 GOAMD64=v3 go build -trimpath -ldflags="-s -w" -o $(BINARY) $(PKG)

build-pgo: $(PGO_PROFILE)
	CGO_ENABLED=0 GOAMD64=v3 go build -pgo=$(PGO_PROFILE) -trimpath -ldflags="-s -w" -o $(BINARY) $(PKG)

pgo:
	@echo "Collecting CPU profiles for PGO..."
	go test -bench=. -benchmem -count=5 -run='^$$' -cpuprofile=hotpath.prof  ./internal/hotpath/
	go test -bench=. -benchmem -count=5 -run='^$$' -cpuprofile=mqtt.prof     ./internal/mqtt/
	go test -bench=. -benchmem -count=5 -run='^$$' -cpuprofile=compress.prof ./internal/compress/
	go tool pprof -proto hotpath.prof mqtt.prof compress.prof > $(PGO_PROFILE)
	rm -f hotpath.prof mqtt.prof compress.prof
	@echo "Profile written to $(PGO_PROFILE) — rebuild with: make build-pgo"

test:
	go test -count=1 -timeout 60s ./...

test-race:
	go test -race -count=1 -timeout 120s ./...

test-cover:
	go test -count=1 -timeout 60s -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1
	@echo "HTML report: go tool cover -html=coverage.out"

lint:
	golangci-lint run

vet:
	go vet ./...

clean:
	rm -f $(BINARY) coverage.out *.prof

docker-build:
	docker build -t $(BINARY):latest .
