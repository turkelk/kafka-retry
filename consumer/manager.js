const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'kafkajs-consumer',
    brokers: ['broker:29092']
});

const RESULT = {
    DLQ: "dlq",
    RETRY: "retry",
    SUCCESS: "success",
    ERROR: "unexpected_error"
}

const sendToTopic = async (message, source, partition, destination) => {

    let msg = { "key": message.key.toString(), "value": message.value.toString(), "headers": message.headers };

    msg.headers.x_offset = message.offset.toString();
    msg.headers.x_topic = source;
    msg.headers.x_partition = partition.toString();
    msg.headers.x_delay_ms = "10000" // from env;

    const producer = kafka.producer();

    await producer.connect();
    await producer.send({
        topic: destination,
        messages: [msg],
    });
    await producer.disconnect();
};

const sendToRetryQueue = async (consumerContext) => {

    const { message, topic, partition } = consumerContext;

    const { x_retry_count } = message.headers;

    if (x_retry_count) {
        let retryCount = parseInt(x_retry_count);
        retryCount++;
        message.headers.x_retry_count = retryCount.toString();
    } else {
        message.headers.x_retry_count = "1";
    }

    await sendToTopic(message, topic, partition, topic + "-retry");
};

const sendToDLQ = async (consumerContext) => {
    const { message, topic, partition } = consumerContext;
    await sendToTopic(message, topic, partition, topic + "-dlq");
};

const handleResult = async (result, consumerContext) => {
    try {
        const { isSuccess, retry } = result;
        if (isSuccess)
            return {
                code: RESULT.SUCCESS,
                message: "message handler success",
                messageHandlerResult: result,
            };

        if (!retry) {
            await sendToDLQ(consumerContext);
            return {
                code: RESULT.DLQ,
                message: "sent to dlq. handler result is not to retry",
                messageHandlerResult: result,
            }
        }

        const { message } = consumerContext;

        const { x_retry_count } = message.headers;
        const maxNumberOfRetryAttempt = 3; // from env

        if (x_retry_count) {
            let retryCount = parseInt(x_retry_count);
            if (retryCount > maxNumberOfRetryAttempt) {
                await sendToDLQ(consumerContext);
                return {
                    code: RESULT.DLQ,
                    message: `exceeded max number of ${maxNumberOfRetryAttempt} retry attempt`,
                    messageHandlerResult: result,
                }
            }
        }

        await sendToRetryQueue(consumerContext);

        return {
            code: RESULT.RETRY,
            message: `sent to retry for ${maxNumberOfRetryAttempt} attempt`,
            messageHandlerResult: result,
        }

    } catch (error) {
        return {
            code: RESULT.ERROR,
            message: 'unexpected error occured',
            messageHandlerResult: result,
            errorDetails: error
        }
    }
};

const handle = async (consumerContext, callback) => {

    const { timeoutInSeconds, messageHandler, resolveOffset, heartbeat, message } = consumerContext;

    let timeoutHandle;

    const timeoutPromise = new Promise((resolve, reject) => {
        timeoutHandle = setTimeout(() => reject(
            {
                retry: true,
                code: "timeout",
                isSuccess: false,
                message: "message handling timeout"
            }), timeoutInSeconds * 1000);
    });

    Promise.race([
        messageHandler(message),
        timeoutPromise,
    ]).then(async function (result) {
        return callback(await handleResult(result, consumerContext));
    }).catch(async function (error) {
        return callback(await handleResult(error, consumerContext));
    }).finally(async function () {
        clearTimeout(timeoutHandle);
        await heartbeat();
        resolveOffset(message.offset);
    })
};

module.exports.handle = handle;