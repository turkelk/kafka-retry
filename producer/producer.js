const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'kafkajs-producer',
    brokers: ['broker:29092']
});

const producer = kafka.producer();

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

const messages = [{ "key": "1", "value": '{ "messageType": "normal" }' }, { "key": "2", "value": '{ "messageType": "retry" }' }, { "key": "3", "value": '{ "messageType": "dlq" }' }, { "key": "4", "value": '{ "messageType": "long-running" }'}];

async function produce(message) {
    await producer.connect();
    await producer.send({
        topic: 'test',
        messages: [message],
    });
    await producer.disconnect();
};

(async function sendMessage() {
    while (true) {
        console.log("sending message");
        const random = Math.floor(Math.random() * messages.length);
        const message = messages[random];
        console.log("sending message", message);
        await produce(message);
        await sleep(5000);
    }
})();

