const moment = require('moment');
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

const delayInMs = 10000; // from env

const sendToTopic = async (message, source, partition, destination) => {

    let msg = {
        key: message.key.toString(),
        value: ""
    };
    
    // var expireAt = new Date();
    // expireAt.setTime(expireAt.getTime() + delayInMs);

    // var d = new Date();
    // d.setDate(reqDate.getDate() + 1);
    // var expireAtISO = moment(expireAt).format('YYYY-MM-DD[T00:00:00.000Z]');    

    let value = {
        payload: message.value.toString(),
        headers: {
            offset: message.offset,
            source: source,
            partition: partition,
            delay: delayInMs,
            retryCount: 0
        },
        expireAt: Date.now()//expireAt.toISOString().toString()//new Date(Date.now()).toISOString()//Date.now()//moment(expireAt).format('YYYY-MM-DD[T00:00:00.000Z]')
    }

    msg.value = JSON.stringify(value);

    const producer = kafka.producer();

    console.log("sending message", msg);

    await producer.connect();
    await producer.send({
        topic: destination,
        messages: [msg],
    });

    console.log(`message ${msg} sent to ${destination}`);

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

    await sendToTopic(message, topic, partition, topic == topic + "-retry" ? topic : topic + "-retry");
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

const process = async (consumerContext, callback) => {

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

module.exports.process = process;