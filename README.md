# Building and Running

```console
$ sh run.sh
```

kafka-setup in docker compose is a workaround to pre-create topics. container will not start a broker.

### If mongo-express gets slave of error, restarting solves the problem

```console
$ docker-compose restart mongo-express
```