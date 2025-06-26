docker-image-agentscope:
	docker build -t wanwulite/agentscope:$(shell date +%Y%m%d)-$(shell git rev-parse --short HEAD) .