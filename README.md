# IoT Bpm Process Mining Processing

Pipeline applying a heuristic mining algorithm on an event log


## Getting started

Add `src/main/resources/kafka.config` with:
```
bootstrap.servers=
group.id=FlinkProcessMining
request.timeout.ms=60000

sasl.mechanism=PLAIN
security.protocol=SASL_SSL
sasl.jaas.config=
```

Add `src/main/resources/mongodb.config` with:
```
mongodb.scheme = mongodb+srv
mongodb.hosts = 
mongodb.userName = 
mongodb.password = 
mongodb.connectionOptions = 
mongodb.uri = 
```
Make sure that the mongodb instance runs as replica set, otherwise the change-stream feature is not available.

## Deployment on Kubernetes

Before building do not forget packaging.

```
docker build . -t acrbpmeventprocessingdev.azurecr.io/iot-bpm-process-mining:dev-release-1.0.0

az acr login --name acrbpmeventprocessingdev.azurecr.io
docker push acrbpmeventprocessingdev.azurecr.io/iot-bpm-process-mining:dev-release-1.0.0

kubectl create -f deployment/bpm-process-mining.yaml
kubectl delete -f deployment/bpm-process-mining.yaml
```