.PHONY: deps test lint lint-check-deps

deps: 
	@echo "[go get] fetching package dependencies"
	@go get -t -v ./...

test: 
	@echo "[go test] running tests and collecting coverage metrics"
	@go test -v -cover -race ./...

lint: lint-check-deps
	@echo "[golangci-lint] linting sources"
	@golangci-lint run -E misspell ./...

lint-check-deps:
	@if [ -z `which golangci-lint` ]; then \
		echo "[go get] installing golangci-lint";\
		go get -u github.com/golangci/golangci-lint/cmd/golangci-lint;\
	fi
