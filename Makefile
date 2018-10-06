.PHONY: all
all: build test

.PHONY: build
build:
	sbt compile

.PHONY: test
test:
	sbt test

.PHONY: docker-build
docker-build:
	sbt docker:publishLocal
