test:
	go test -timeout 10s -cover ./warehouse

lint:
	golangci-lint run warehouse
