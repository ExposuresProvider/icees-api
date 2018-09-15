sudo systemctl stop icees-api-container
docker rm icees-api_server
docker build . -t icees-api:0.1.0 --no-cache
docker run -d -e ICEES_DBUSER=$ICEES_DBUSER -e ICEES_DBPASS=$ICEES_DBPASS -e ICEES_HOST=$ICEES_HOST -e ICEES_PORT=$ICEES_PORT -e ICEES_DATABASE=$ICEES_DATABASE --name icees-api_server --net host icees-api:0.1.0
docker stop icees-api_server
sudo cp icees-api-container.service /etc/systemd/system/icees-api-container.service
sudo systemctl daemon-reload
sudo systemctl start icees-api-container
