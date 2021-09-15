const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'kafkajs-consumer',
    brokers: ['broker:29092']
});

const consumer = kafka.consumer({ groupId: 'test-group' });

// const { HEARTBEAT } = consumer.events;
// const removeListener = consumer.on(HEARTBEAT, e => console.log(`heartbeat at ${e.timestamp}`))

const config = { "maxRetry": 3, "retryQueue": "test-retry", "delayDurationInMs": 1000, "dlq": "test-dlq" };

const sendToTopic = async (message, fromTopic, partition, toTopic, reason) => {

    console.log("sending message", message.value.toString(), fromTopic, toTopic, partition, reason);

    let msg = { "key": message.key.toString(), "value": message.value.toString(), "headers": message.headers };

    msg.headers.x_offset = message.offset.toString();
    msg.headers.x_topic = fromTopic;
    msg.headers.x_partition = partition.toString();
    msg.headers.x_delay_duration = config.delayDurationInMs.toString();
    msg.headers.x_reason = reason;

    const { x_retry_count } = message.headers;

    if (x_retry_count) {
        let count = parseInt(x_retry_count);
        count++;
        msg.headers.x_retry_count = count.toString();
    } else {
        msg.headers.x_retry_count = "1";
    }

    console.log("sending message to topic", msg, toTopic);

    const producer = kafka.producer();

    await producer.connect();
    await producer.send({
        topic: toTopic,
        messages: [msg],
    });
    await producer.disconnect();
};

const handleRetry = async (message, topic, partition, reason) => {

    console.log("retrying, message, topic, partition, reason", message.value.toString(), topic, partition, reason);

    const { x_retry_count } = message.headers;

    console.log("handleRetry x_retry_count is", x_retry_count);

    if (x_retry_count) {
        if (x_retry_count >= config.maxRetry) {
            await handleDLQ(message, topic, partition, "number of retry exceeded");
            return;
        }
    }
    console.log("retrying, sending to topic", topic);
    await sendToTopic(message, topic, partition, config.retryQueue, "reasonMessage");
};

const handleDLQ = async (message, topic, partition, reason) => {
    await sendToTopic(message, topic, partition, config.dlq, reason);
};

(async function run() {
    await consumer.connect();
    await consumer.subscribe({ topic: 'test', fromBeginning: false });
    await consumer.subscribe({ topic: 'test-retry', fromBeginning: false });
    // await consumer.subscribe({ topic: 'test-dlq', fromBeginning: false });

    consumer.run({

        eachBatchAutoResolve: false,

        eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {

            for (let message of batch.messages) {
                if (!isRunning() || isStale()) break;
                let msgVal = JSON.parse(message.value.toString());
                if (msgVal.messageType === "retry")
                    await handleRetry(message, batch.topic, batch.partition, "message is retry");
                if (msgVal.messageType === "dlq") {
                    await handleDLQ(message, batch.topic, batch.partition, "message is dlq");
                }
                resolveOffset(message.offset);
                await heartbeat();
            }
        }
    });
})();



