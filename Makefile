.PHONY: all build clean test bench cover cover-out docker_build docker_run

all: build

build:
	go build ./cmd/bumble
	go build ./cmd/bumble_client
	go build ./cmd/bumble_stress

clean:
	rm -f bumble bumble_client bumble_stress
	rm -rf data data-recovery db.log snipped zipped

test:
	go test ./test/* -v -race

bench:
	go test ./pkg/* -bench=.

cover:
	go test ./pkg/* -coverprofile=coverage.out

cover-out:
	go tool cover -html=coverage.out

format:
	gofmt -s -w .

docker_build:
	docker build -t bumblebase .

docker_run:
	docker run --rm -v ${shell pwd}/data:/app/data -it bumblebase
