.PHONY: build

build:
	@docker build -t maisa/spark-base-hadoop ./cluster-hadoop/hadoop/spark-base
	@docker build -t maisa/spark-master-hadoop ./cluster-hadoop/hadoop/spark-master
	@docker build -t maisa/spark-worker-hadoop ./cluster-hadoop/hadoop/spark-worker

