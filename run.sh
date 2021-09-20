#!/bin/bash

echo "Starting docker ."
docker-compose up -d --build

echo -e "\nConfiguring the MongoDB ReplicaSet.\n"
docker-compose exec mongo1 /usr/bin/mongo --eval '''if (rs.status()["ok"] == 0) {
    rsconf = {
      _id : "rs0",
      members: [
        { _id : 0, host : "mongo1:27017", priority: 1.0 }
      ]
    };
    rs.initiate(rsconf);
}
rs.conf();rs.slaveOk();'''

echo -e "\nKafka Topics:"
curl -X GET "http://localhost:8082/topics" -w "\n"

echo -e "\nKafka Connectors:"
curl -X GET "http://localhost:8083/connectors/" -w "\n"

sleep 5

echo -e "\nAdding MongoDB Kafka Sink Connector for the 'test-retry' topic into the 'test.retry' collection:"
curl -X POST -H "Content-Type: application/json" --data '
  {"name": "mongo-sink",
   "config": {
     "connector.class":"com.mongodb.kafka.connect.MongoSinkConnector",
     "tasks.max":"1",
     "topics":"test-retry",
     "connection.uri":"mongodb://mongo1:27017",
     "database":"test",
     "collection":"retry",
     "key.converter": "org.apache.kafka.connect.storage.StringConverter",
     "value.converter": "org.apache.kafka.connect.json.JsonConverter",
     "value.converter.schemas.enable": "false",
     "mongo.errors.tolerance":"all",
     "mongo.errors.log.enable": "true",
     "errors.log.include.messages":"true",
     "errors.deadletterqueue.topic.name": "test-dlq",
     "errors.deadletterqueue.context.headers.enable": "true"
}}' http://localhost:8083/connectors -w "\n"


sleep 2
echo -e "\nAdding MongoDB Kafka Source Connector for the 'test.retry' collection:"
curl -X POST -H "Content-Type: application/json" --data '
  {"name": "mongo-source",
   "config": {
     "tasks.max":"1",
     "connector.class":"com.mongodb.kafka.connect.MongoSourceConnector",
     "connection.uri":"mongodb://mongo1:27017",
     "topic.prefix":"mongo",
     "database":"test",
     "collection":"retry"
}}' http://localhost:8083/connectors -w "\n"    

sleep 2
echo -e "\nKafka Connectors: \n"
curl -X GET "http://localhost:8083/connectors/" -w "\n"

echo "Looking at data in 'db.retry':"
docker-compose exec mongo1 /usr/bin/mongo --eval 'db.retry.find()'


echo -e '''
==============================================================================================================
Examine the topics in the Kafka UI: http://localhost:9021 or http://localhost:8000/
  - The `retry` topic should have the generated retry.
  - The `mongo.test.retry` topic should contain the change events.
  - The `test.retry` collection in MongoDB should contain the sinked page views.
Examine the collections:
  - In your shell run: docker-compose exec mongo1 /usr/bin/mongo
Examine the collections:http://localhost:8888/ 
==============================================================================================================

Use <ctrl>-c to quit'''

read -r -d '' _ </dev/tty