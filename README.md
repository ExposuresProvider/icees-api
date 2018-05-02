### build container

```
docker build . -t ddcr-api:0.1.0
```

### run container

```
docker run --rm -p 8080:8080 ddcr-api:0.1.0
```

### setting up systemd

run docker containers
```
docker run -d --name ddcr-api_server -p 8080:8080 ddcr-api:0.1.0
```

add following file to `/etc/systemd/system/ddcr-api-container.service`

```
[Unit]
Description=DDCR API container
After=docker.service

[Service]
Restart=always
ExecStart=/usr/bin/docker start -a ddcr-api_server
ExecStop=/usr/bin/docker stop -t 2 ddcr-api_server

[Install]
WantedBy=local.target
```