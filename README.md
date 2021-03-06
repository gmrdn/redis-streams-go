# redis-streams-go

# Why ?

This project is a basic example of a publisher and consumers of a redis stream

# Local env

Start a local redis with :
```
docker run --name mediumredis -d redis redis-server --appendonly yes
```

# Publisher

The publisher sends 3000 messages to the redis stream
```
cd publisher
go run main.go
```
