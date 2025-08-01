docker-image-agentscope-base:
	docker build -f Dockerfile.develop -t wanwulite/agentscope-base:$(shell date +%Y%m%d)-$(shell git rev-parse --short HEAD) .

docker-image-agentscope:
	docker build -t wanwulite/agentscope:$(shell date +%Y%m%d)-$(shell git rev-parse --short HEAD) .