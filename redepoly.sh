sudo systemctl stop icees-api-container
docker rm icees-api_server
docker build . -t icees-api:0.1.0 --no-cache
docker run -d -e ICEES_DBUSER -e ICEES_DBPASS -e ICEES_HOST -e ICEES_PORT -e ICEES_DATABASE -e ICEES_API_LOG_PATH=/log/icees --name icees-api_server -v $ICEES_API_LOG_PATH:/log --net host icees-api:0.2.0
docker stop icees-api_server
sudo cp icees-api-container.service /etc/systemd/system/icees-api-container.service
sudo systemctl daemon-reload
sudo systemctl start icees-api-container
