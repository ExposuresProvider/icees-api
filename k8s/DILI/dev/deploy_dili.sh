#!/usr/bin/env bash

kubectl apply -f dili-config.yaml
kubectl apply -f server-service.yaml,redis-service.yaml,server-deployment.yaml,redis-deployment.yaml,server-ingress.yaml
