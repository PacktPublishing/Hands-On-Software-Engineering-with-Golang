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

mocks:
	@if [ -z `which mock-gen` ]; then \
		echo "[go get] installing gomock/mockgen";\
		go get github.com/golang/mock/gomock;\
		go install github.com/golang/mock/mockgen;\
	fi
	@echo "[mockgen] creating mocks"
	mockgen \
		-destination mock/dependency.go \
		github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/ch04/dependency \
		API

