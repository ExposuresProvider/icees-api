#!/usr/bin/env bash

kubectl --namespace icees-dev apply -f covid-config.yaml
kubectl --namespace icees-dev apply -f redis-pvc.yaml,server-service.yaml,redis-service.yaml,server-deployment.yaml,redis-deployment.yaml,server-ingress.yaml
