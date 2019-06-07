.PHONY: deps test lint lint-check-deps ci-check

deps: 
	@echo "[go get] fetching package dependencies"
	@go get -t -v ./...

test: 
	@echo "[go test] running tests and collecting coverage metrics"
	@go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...

lint: lint-check-deps
	@echo "[golangci-lint] linting sources"
	@golangci-lint run \
		-E misspell \
		-E golint \
		-E gofmt \
		-E unconvert \
		--exclude-use-default=false \
		./...

lint-check-deps:
	@if [ -z `which golangci-lint` ]; then \
		echo "[go get] installing golangci-lint";\
		go get -u github.com/golangci/golangci-lint/cmd/golangci-lint;\
	fi

ci-check: deps lint test
