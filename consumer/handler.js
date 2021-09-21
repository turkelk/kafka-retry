function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
};

const handle = async (message) => {
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

module.exports.handle = handle;