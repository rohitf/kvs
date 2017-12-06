docker kill $(docker ps -q)
docker build -t asgn4 .
docker run -p 8083:8080 --ip=10.0.0.20 --net=mynet -e K=3 -e VIEW=10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080 -e IPPORT=10.0.0.20:8080 asgn3
