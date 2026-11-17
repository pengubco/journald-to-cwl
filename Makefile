.PHONY: build-go build-rpm test lint update-go

build-go:
	go build -o build/journald-to-cwl github.com/pengubco/journald-to-cwl

build-rpm: build-go
	cp build/journald-to-cwl ./rpmbuild/SOURCES
	rpmbuild -bb --define "_topdir ${PWD}/rpmbuild" ./rpmbuild/SPECS/journald-to-cwl.spec

test:
	go test -timeout 30s -race -vet=all -v -count=1 github.com/pengubco/journald-to-cwl/...

# Requires golangci-lint v2: go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
lint:
	golangci-lint run ./...

# Pins the toolchain in go.mod to the latest stable Go release, then lints and tests with it. The go directive (the
# minimum Go version for users of this module) is left unchanged. With the default GOTOOLCHAIN=auto, an older local
# go downloads the pinned toolchain on first use.
update-go:
	go get toolchain@latest
	go mod tidy
	go version
	$(MAKE) lint test
