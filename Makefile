# 定义变量
IMAGE_NAME ?= registry.ai-yuanjing.cn/maas/agentscope
TODAY := $(shell date +%Y%m%d)
VERSION ?= $(shell git describe --tags --always)-$(TODAY)

PWD=$(shell pwd)

.PHONY: clean
clean:
	docker system prune -f

.PHONY: dockerbuild
dockerbuild: check-docker
	docker build --no-cache --platform linux/amd64 -f Dockerfile -t $(IMAGE_NAME):${VERSION} .

.PHONY: dockerpush
dockerpush: check-docker
	docker push $(IMAGE_NAME):${VERSION}

.PHONY: check-docker
check-docker:
	@which docker || (echo "Docker not found. Please install Docker first." && exit 1)