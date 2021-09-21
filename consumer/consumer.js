const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'kafkajs-consumer',
    brokers: ['broker:29092']
});

const consumer = kafka.consumer({ groupId: 'test-group' });
const processor = require('./processor');
const config = { messageHandleTimeoutInSeconds: 20 };

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
};

// example handler....
const handleMessage = async (message) => {
    let msgVal = JSON.parse(message.value.toString());

    switch (msgVal.messageType) {
        case "normal":
            return Promise.resolve({
                retry: false,
                code: "success",
                isSuccess: true,
                message: "completed successfully"
            });
        case "retry":
            return Promise.resolve({
                retry: true,
                code: "retry_message",
                isSuccess: false,
                message: "message is retry"
            });
        case "long-running":

            await sleep(config.messageHandleTimeoutInSeconds + 5);

            return Promise.resolve({
                retry: false,
                code: "success",
                isSuccess: true,
                message: "completed successfully"
            });
        case "dlq":
            return Promise.resolve({
                retry: false,
                code: "dlq_message",
                isSuccess: false,
                message: "message is dlq message"
            })

        default:
            return Promise.resolve({
                retry: false,
                code: "success",
                isSuccess: true,
                message: "completed successfully"
            });
    };
};

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
                    messageHandler: handleMessage,
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