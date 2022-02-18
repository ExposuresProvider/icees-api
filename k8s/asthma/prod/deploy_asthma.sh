#!/usr/bin/env bash

kubectl --namespace icees-prod apply -f asthma-config.yaml
kubectl --namespace icees-prod apply -f server-service.yaml,redis-service.yaml,server-deployment.yaml,redis-deployment.yaml,server-ingress.yaml
