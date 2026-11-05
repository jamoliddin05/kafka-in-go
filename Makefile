SHELL := /bin/bash

# Defaults come from .env so `make dev` remembers the task you're on.
T     ?= $(shell grep -E '^TASK=' .env 2>/dev/null | cut -d= -f2)
R     ?= producer consumer
N     ?= 3
V     ?= v1
PAUSE ?= 15

COMPOSE := docker compose
KAFKA   := $(COMPOSE) exec -T kafka-1 /opt/kafka/bin
BS      := --bootstrap-server kafka-1:29092
INFRA   := kafka-1 kafka-2 kafka-3 kafka-ui jaeger postgres
APPS    := api producer processor relay consumer
WORKERS := worker-a worker-b worker-c

.DEFAULT_GOAL := help
.PHONY: help up dev scale logs stop-app down reset ps topics describe groups group tail \
        broker-stop broker-start broker-kill prod-image workers rollout crash psql

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- environment

up: ## Start Kafka x3, Kafka UI, Jaeger, Postgres and wait until healthy
	$(COMPOSE) up -d --wait $(INFRA)
	@echo "Kafka UI  http://localhost:8085"
	@echo "Jaeger    http://localhost:16686"
	@echo "Brokers   localhost:9092,localhost:9093,localhost:9094 (from host)"

dev: ## Run task T roles R with hot reload: make dev T=05-consumer-groups R="producer consumer"
	TASK=$(T) $(COMPOSE) up --build $(R)

scale: ## Run N replicas of one role in the background: make scale R=consumer N=3
	TASK=$(T) $(COMPOSE) up -d --build --scale $(R)=$(N) $(R)

logs: ## Follow logs of roles R: make logs R=consumer
	$(COMPOSE) logs -f --tail=50 $(R)

stop-app: ## Stop all Go processes (dev and prod), keep Kafka running
	$(COMPOSE) --profile app --profile prod rm -sf $(APPS) $(WORKERS)

ps: ## Show containers
	$(COMPOSE) --profile '*' ps

down: ## Stop everything and wipe Kafka data (Go module/build caches are kept)
	@# rm -v drops the anonymous data volumes the Kafka image declares; plain `down` would leak them.
	$(COMPOSE) --profile '*' rm -sfv $(INFRA)
	$(COMPOSE) --profile '*' down --remove-orphans

reset: ## Stop everything and wipe all volumes, including Go module/build caches
	$(COMPOSE) --profile '*' down -v --remove-orphans

# ---------------------------------------------------------------- kafka CLI

topics: ## List topics
	$(KAFKA)/kafka-topics.sh $(BS) --list

describe: ## Partitions, leaders, replicas, ISR of a topic: make describe TOPIC=t01.events
	$(KAFKA)/kafka-topics.sh $(BS) --describe --topic $(TOPIC)
	$(KAFKA)/kafka-configs.sh $(BS) --describe --entity-type topics --entity-name $(TOPIC)

groups: ## List consumer groups
	$(KAFKA)/kafka-consumer-groups.sh $(BS) --list

group: ## Committed offsets, lag and members of a group: make group G=t05.billing
	$(KAFKA)/kafka-consumer-groups.sh $(BS) --describe --group $(G)
	$(KAFKA)/kafka-consumer-groups.sh $(BS) --describe --group $(G) --members --verbose

tail: ## Print a topic from the beginning with partition, offset, key, headers: make tail TOPIC=t01.events
	$(KAFKA)/kafka-console-consumer.sh $(BS) --topic $(TOPIC) --from-beginning \
		--formatter-property print.partition=true --formatter-property print.offset=true \
		--formatter-property print.key=true --formatter-property print.headers=true \
		--formatter-property print.timestamp=true

broker-stop: ## Gracefully stop broker B (1..3): make broker-stop B=2
	$(COMPOSE) stop kafka-$(B)

broker-kill: ## SIGKILL broker B, like a node dying: make broker-kill B=2
	$(COMPOSE) kill -s KILL kafka-$(B)

broker-start: ## Start broker B again: make broker-start B=2
	$(COMPOSE) start kafka-$(B)

# ---------------------------------------------------------------- prod image / deploys

prod-image: ## Build the prod image (no air, distroless) for task T: make prod-image T=07-graceful-shutdown V=v2
	docker build --target prod --build-arg TASK=$(T) -t kafka-course/$(T):$(V) .

workers: ## Start 3 prod-image replicas of task T version V: make workers T=07-graceful-shutdown
	TASK=$(T) VERSION=$(V) $(COMPOSE) --profile prod up -d --build $(WORKERS)

rollout: ## Rolling deploy to version V, one replica at a time: make rollout V=v2 PAUSE=15
	TASK=$(T) VERSION=$(V) $(COMPOSE) --profile prod build worker-a
	@for w in $(WORKERS); do \
		echo ">>> deploying $$w -> $(V)"; \
		TASK=$(T) VERSION=$(V) $(COMPOSE) --profile prod up -d --no-deps --no-build $$w; \
		sleep $(PAUSE); \
	done

crash: ## SIGKILL one worker (OOM-kill / node loss): make crash W=worker-b
	$(COMPOSE) kill -s KILL $(W)

psql: ## Open psql in the course database
	$(COMPOSE) exec postgres psql -U course course
