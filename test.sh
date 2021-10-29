#docker run --name redis -d --net host --rm redis
docker run --name redis -p 6379:6379 -d redis
export CONFIG_PATH=./test/config
export DB_PATH=./test/example.db
export ICEES_API_LOG_PATH=./logs
mkdir ./logs
python -m pytest --cov=icees_api --cov-report=xml -vvvv test/
docker rm -f redis
