#!/usr/bin/env bash

kubectl --namespace icees-dev apply -f dili-config.yaml
kubectl --namespace icees-dev apply -f server-service.yaml,redis-service.yaml,server-deployment.yaml,redis-deployment.yaml,server-ingress.yaml
