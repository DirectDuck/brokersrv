NAME := brokersrv
PKG := `go list -f {{.Dir}} ./...`

MAIN := cmd/${NAME}/main.go

fmt:
	@golangci-lint fmt

test:
	@go test -v ./...

lint:
	@golangci-lint version
	@golangci-lint config verify
	@golangci-lint run

build:
	@CGO_ENABLED=0 go build $(GOFLAGS) -o ${NAME} $(MAIN)

run:
	@echo "Compiling"
	@go run -buildvcs=true $(GOFLAGS) $(MAIN) -config=cfg/local.toml -dev

mod:
	@go mod tidy
