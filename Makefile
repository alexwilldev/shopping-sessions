# Local loop. PLATFORM=linux/arm64 is fast on the M1; build linux/amd64 before submitting.
IMAGE    ?= shopping-sessions:latest
PLATFORM ?= linux/arm64
CPUS     ?= 8
MEM      ?= 7g
export IMAGE PLATFORM CPUS MEM

.PHONY: build build-amd64 check check-amd64 bench20 bench60 fuzz

build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

build-amd64:
	docker buildx build --platform linux/amd64 --load -t $(IMAGE)-amd64 .

## Submission gate: 36 fixtures + dev set vs dev answer
check: build
	scripts/check.sh $(IMAGE)

## Correctness on the real scorer platform (emulated, slow)
check-amd64: build-amd64
	PLATFORM=linux/amd64 scripts/check.sh $(IMAGE)-amd64

bench20: build
	scripts/bench.sh $(IMAGE) data/gen20m 3

bench60: build
	scripts/bench.sh $(IMAGE) data/gen60m 1

## Differential test vs a reference image: make fuzz REF=shopping-sessions:v1
REF ?= shopping-sessions:v1
fuzz: build
	scripts/fuzz.sh $(REF) $(IMAGE) 50

## Reference (v1 Polars) image for fuzzing: make build-v1
build-v1:
	docker buildx build --platform $(PLATFORM) --load -f Dockerfile.polars -t shopping-sessions:v1 .
