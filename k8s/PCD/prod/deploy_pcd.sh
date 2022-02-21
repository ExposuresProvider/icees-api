#!/usr/bin/env bash

kubectl --namespace icees-prod apply -f pcd-config.yaml
kubectl --namespace icees-prod apply -f redis-pvc.yaml,server-service.yaml,redis-service.yaml,server-deployment.yaml,redis-deployment.yaml,server-ingress.yaml
