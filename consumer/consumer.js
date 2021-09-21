const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'kafkajs-consumer',
    brokers: ['broker:29092']
});

const consumer = kafka.consumer({ groupId: 'test-group' });
const processor = require('./processor');
const config = { messageHandleTimeoutInSeconds: 20 };

// example handler....
const messageHandler = require('./handler');


(async function run() {
    await consumer.connect();
    await consumer.subscribe({ topic: 'test', fromBeginning: false });
    await consumer.subscribe({ topic: 'test-retry', fromBeginning: false });

    consumer.run({

        eachBatchAutoResolve: false,

        eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {

            for (let message of batch.messages) {
                if (!isRunning() || isStale()) break;

                const consumerContext = {
                    timeoutInSeconds: config.messageHandleTimeoutInSeconds,
                    messageHandler: messageHandler.handle,
                    resolveOffset,
                    heartbeat,
                    message,
                    topic: batch.topic,
                    partition: batch.partition
                }
                processor.process(consumerContext, (result) => {
                    console.log("consumerHandler.handle is", result);
                });
            }
        }
    });
})();