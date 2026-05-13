# ─────────────────────────────────────────────────────────────
#  opencode-gateway  Makefile
#  Works from Git Bash AND PowerShell on Windows, and Linux/macOS
# ─────────────────────────────────────────────────────────────
#
#  Strategy:
#   - No SHELL override: use the system default shell (sh in Git Bash,
#     cmd.exe in native make, /bin/sh on Linux/macOS).
#   - Avoid all shell-specific commands (mkdir -p, rm -rf, cp, etc.)
#     in recipes. Use `go run` helpers instead so they work everywhere.
#   - Only `go build`, `go test`, `go vet`, `echo`, and `docker` are
#     used in recipes — all universally available.
#
# ─────────────────────────────────────────────────────────────

# ── Project metadata ──────────────────────────────────────────
# Use go run so these work identically on any OS/shell.
MODULE   := github.com/user/opencode-gateway
VERSION  := $(shell go run tools/buildinfo/main.go version)
COMMIT   := $(shell go run tools/buildinfo/main.go commit)
BUILD_TS := $(shell go run tools/buildinfo/main.go date)
LDFLAGS  := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildTime=$(BUILD_TS)

# ── Paths ─────────────────────────────────────────────────────
BIN_DIR     := bin
INSTALL_DIR := $(shell go env GOPATH)/bin

# ── OS / EXE suffix (auto-detected from the host running make) ─
ifeq ($(OS),Windows_NT)
  EXE := .exe
else
  EXE :=
endif

# ── Output binaries (native platform) ────────────────────────
GATEWAY_BIN := $(BIN_DIR)/openbot$(EXE)
MCP_BIN     := $(BIN_DIR)/openbot-mcp$(EXE)
ATTACH_BIN  := $(BIN_DIR)/openbot-attach$(EXE)

# ── Go helpers (cross-platform, no shell deps) ───────────────
MKDIRP := go run tools/mkdirp/main.go

# ── Env file ─────────────────────────────────────────────────
ENV_FILE    ?= .env

# ══════════════════════════════════════════════════════════════
#  Targets
# ══════════════════════════════════════════════════════════════

.PHONY: all build install uninstall clean run run-env test vet lint \
        build-gateway build-mcp build-attach \
        build-linux build-darwin build-darwin-arm64 build-windows build-all \
        init-env init-dirs help

## Default: build all
all: build

## Build all binaries for the current OS
build: init-dirs build-gateway build-mcp build-attach
	@echo ""
	@echo "===== Build complete ====="
	@echo "  $(GATEWAY_BIN)"
	@echo "  $(MCP_BIN)"
	@echo "  $(ATTACH_BIN)"

build-gateway: init-dirs
	@echo "[BUILD] openbot (gateway)..."
	go build -ldflags "$(LDFLAGS)" -o $(GATEWAY_BIN) ./cmd/gateway

build-mcp: init-dirs
	@echo "[BUILD] openbot-mcp..."
	go build -ldflags "$(LDFLAGS)" -o $(MCP_BIN) ./cmd/mcp

build-attach: init-dirs
	@echo "[BUILD] openbot-attach..."
	go build -ldflags "$(LDFLAGS)" -o $(ATTACH_BIN) ./cmd/attach

# ── Install to GOPATH/bin (add to PATH for CLI access) ────────
## Install binaries to GOPATH/bin
install: build
	$(MKDIRP) $(INSTALL_DIR)
	go run tools/install/main.go $(GATEWAY_BIN) $(INSTALL_DIR)/openbot$(EXE)
	go run tools/install/main.go $(MCP_BIN)     $(INSTALL_DIR)/openbot-mcp$(EXE)
	go run tools/install/main.go $(ATTACH_BIN)  $(INSTALL_DIR)/openbot-attach$(EXE)
	@echo "Installed to $(INSTALL_DIR) — make sure it is in your PATH."

## Uninstall: remove installed binaries
uninstall:
	go run tools/remove/main.go \
		$(INSTALL_DIR)/openbot$(EXE) \
		$(INSTALL_DIR)/openbot-mcp$(EXE) \
		$(INSTALL_DIR)/openbot-attach$(EXE)
	@echo "Uninstalled."

# ── Run ───────────────────────────────────────────────────────
## Run directly (uses current shell env vars)
run: build
	$(GATEWAY_BIN)

## Load .env and run (one-click start)
run-env: build
	go run tools/runenv/main.go $(ENV_FILE) $(GATEWAY_BIN)

# ── Init ──────────────────────────────────────────────────────
## Create bin/ output directory
init-dirs:
	$(MKDIRP) $(BIN_DIR)

## Generate .env from template (won't overwrite existing)
init-env:
	go run tools/initenv/main.go .env.example .env

# ── Code quality ──────────────────────────────────────────────
## Run tests
test:
	go test ./... -v -count=1

## go vet static check
vet:
	go vet ./...

## golangci-lint (install: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
lint:
	golangci-lint run ./...

## Clean build artifacts
clean:
	go run tools/remove/main.go -r $(BIN_DIR)
	@echo "Cleaned."

# ── Docker ────────────────────────────────────────────────────
DOCKER_IMAGE ?= openbot
DOCKER_TAG   ?= $(VERSION)

## Docker build
docker-build:
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .
	@echo "Built $(DOCKER_IMAGE):$(DOCKER_TAG)"

## Docker run (with .env)
docker-run:
	docker run --rm --env-file $(ENV_FILE) -p 8080:8080 $(DOCKER_IMAGE):$(DOCKER_TAG)

# ── Cross-compile ─────────────────────────────────────────────
# Output layout: bin/<os>_<arch>/openbot[.exe]

## linux/amd64 static binary
build-linux:
	$(MKDIRP) $(BIN_DIR)/linux_amd64
	@echo "[BUILD] linux/amd64 ..."
	GOOS=linux   GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/linux_amd64/openbot        ./cmd/gateway
	GOOS=linux   GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/linux_amd64/openbot-mcp    ./cmd/mcp
	GOOS=linux   GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/linux_amd64/openbot-attach ./cmd/attach
	@echo "linux/amd64 -> $(BIN_DIR)/linux_amd64/"

## darwin/amd64 binary
build-darwin:
	$(MKDIRP) $(BIN_DIR)/darwin_amd64
	@echo "[BUILD] darwin/amd64 ..."
	GOOS=darwin  GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/darwin_amd64/openbot        ./cmd/gateway
	GOOS=darwin  GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/darwin_amd64/openbot-mcp    ./cmd/mcp
	GOOS=darwin  GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/darwin_amd64/openbot-attach ./cmd/attach
	@echo "darwin/amd64 -> $(BIN_DIR)/darwin_amd64/"

## darwin/arm64 binary (Apple Silicon)
build-darwin-arm64:
	$(MKDIRP) $(BIN_DIR)/darwin_arm64
	@echo "[BUILD] darwin/arm64 ..."
	GOOS=darwin  GOARCH=arm64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/darwin_arm64/openbot        ./cmd/gateway
	GOOS=darwin  GOARCH=arm64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/darwin_arm64/openbot-mcp    ./cmd/mcp
	GOOS=darwin  GOARCH=arm64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/darwin_arm64/openbot-attach ./cmd/attach
	@echo "darwin/arm64 -> $(BIN_DIR)/darwin_arm64/"

## windows/amd64 binary
build-windows:
	$(MKDIRP) $(BIN_DIR)/windows_amd64
	@echo "[BUILD] windows/amd64 ..."
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/windows_amd64/openbot.exe        ./cmd/gateway
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/windows_amd64/openbot-mcp.exe    ./cmd/mcp
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/windows_amd64/openbot-attach.exe ./cmd/attach
	@echo "windows/amd64 -> $(BIN_DIR)/windows_amd64/"

## Build all platforms at once
build-all: build-linux build-darwin build-darwin-arm64 build-windows
	@echo ""
	@echo "===== All platforms built ====="
	@echo "  $(BIN_DIR)/linux_amd64/"
	@echo "  $(BIN_DIR)/darwin_amd64/"
	@echo "  $(BIN_DIR)/darwin_arm64/"
	@echo "  $(BIN_DIR)/windows_amd64/"

# ── Help ──────────────────────────────────────────────────────
## Show help
help:
	@echo ""
	@echo "============================================================"
	@echo "  opencode-gateway Makefile"
	@echo "============================================================"
	@echo ""
	@echo " Build (native OS):"
	@echo "   make / make build        Build for current OS to bin/"
	@echo ""
	@echo " Cross-compile:"
	@echo "   make build-linux          linux/amd64"
	@echo "   make build-darwin         darwin/amd64"
	@echo "   make build-darwin-arm64   darwin/arm64 (Apple Silicon)"
	@echo "   make build-windows        windows/amd64"
	@echo "   make build-all            All platforms at once"
	@echo ""
	@echo " Install:"
	@echo "   make install      Install to GOPATH/bin (CLI-ready)"
	@echo "   make uninstall    Remove installed binaries"
	@echo ""
	@echo " Run:"
	@echo "   make run          Build and run (current env vars)"
	@echo "   make run-env      Load .env then run (one-click)"
	@echo ""
	@echo " Init:"
	@echo "   make init-env     Generate .env from .env.example"
	@echo ""
	@echo " Quality:"
	@echo "   make test         Run tests"
	@echo "   make vet          go vet"
	@echo "   make lint         golangci-lint"
	@echo ""
	@echo " Docker:"
	@echo "   make docker-build Build Docker image"
	@echo "   make docker-run   Run with .env in Docker"
	@echo ""
	@echo " Other:"
	@echo "   make clean        Remove build artifacts"
	@echo "   make help         Show this help"
	@echo ""
	@echo "============================================================"
	@echo " Quick start:"
	@echo "   1. make init-env    Generate config file"
	@echo "   2. Edit .env        Fill in your credentials"
	@echo "   3. make install     Build and install to PATH"
	@echo "   4. make run-env     One-click start with .env"
	@echo "============================================================"
	@echo ""

