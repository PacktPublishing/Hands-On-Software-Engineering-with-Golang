include ../../../Makefile

.PHONY: dockerize push dockerize-and-push
	
SHELL=/bin/bash -o pipefail
IMAGE = linksrus-textindexer
SHA = $(shell git rev-parse --short HEAD)

ifeq ($(origin PRIVATE_REGISTRY),undefined)
PRIVATE_REGISTRY := $(shell minikube ip 2>/dev/null):5000
endif

ifneq ($(PRIVATE_REGISTRY),)
	PREFIX:=${PRIVATE_REGISTRY}/
endif

dockerize-and-push: dockerize push

dockerize:
	@echo "[docker build] building ${IMAGE} (tags: ${PREFIX}${IMAGE}:latest, ${PREFIX}${IMAGE}:${SHA})"
	@docker build --file ./Dockerfile \
		--tag ${PREFIX}${IMAGE}:latest \
		--tag ${PREFIX}${IMAGE}:${SHA} \
		../../../ 2>&1 | sed -e "s/^/ | /g"

push:
	@echo "[docker push] pushing ${PREFIX}${IMAGE}:latest"
	@docker push ${PREFIX}${IMAGE}:latest 2>&1 | sed -e "s/^/ | /g"
	@echo "[docker push] pushing ${PREFIX}${IMAGE}:${SHA}"
	@docker push ${PREFIX}${IMAGE}:${SHA} 2>&1 | sed -e "s/^/ | /g"
