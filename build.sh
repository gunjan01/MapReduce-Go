# Script to build your protobuf, go binaries, and docker images here
# Generating proto code.
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    protos/mapreduce.proto

echo "Build docker images"
docker build -t master -f docker/Dockerfile.master .
docker build -t worker -f docker/Dockerfile.worker .    
docker build -t http -f docker/Dockerfile.http .    

# Run docker containers
echo "Starting the etc local container"
docker run -it -d --env ALLOW_NONE_AUTHENTICATION=yes -p 2379:2379 -p 2380:2380  bitnami/etcd:latest

echo "Starting the worker container"
#docker run --net=host -it -d worker:latest &
docker run --env WORKER_IP=test  --net=host -it worker:latest
docker run --env PORT=8081  --net=host -it worker:latest
docker run --env PORT=8082  --net=host -it worker:latest

echo "Starting the master container"
docker run --net=host -it master:latest

# Build and push images to docker hub"
docker tag worker:latest gunjantank11/workshop8:worker-tag
docker push  gunjantank11/workshop8:worker-tag     
docker tag master:latest gunjantank11/workshop8:master-tag
docker push gunjantank11/workshop8:master-tag 



docker run --net=host -it worker:latest 8080 0
docker run --net=host -it worker:latest 8081 0
docker run --net=host -it worker:latest 8082 0

# Retrieve keys from etcd
docker exec -it 23dca1b0de92 etcdctl get Leader     
docker exec -it 23dca1b0de92 etcdctl get --prefix Job_


Reducer task map is map[0:[./intermediate/map00 ./intermediate/map01 ./intermediate/map02 ./intermediate/map03] 1:[./intermediate/map04 ./intermediate/map05 ./intermediate/map06 ./intermediate/map07] 2:[./intermediate/map08 ./intermediate/map09 ./intermediate/map10 ./intermediate/map11] 3:[./intermediate/map12 ./intermediate/map13 ./intermediate/map14 ./intermediate/map15] 4:[./intermediate/map16 ./intermediate/map17 ./intermediate/map18]]
killall Docker && open /Applications/Docker.app


