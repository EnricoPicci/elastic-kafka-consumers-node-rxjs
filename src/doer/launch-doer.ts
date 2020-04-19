import { startDoer } from './start-doer';

function launchDoer() {
    const name = process.argv[2];
    console.log('name', name);
    const id = parseInt(process.argv[3]);
    console.log('id', id);
    const brokers = [process.argv[4]];
    console.log('brokers', brokers);
    const inputTopic = process.argv[5];
    console.log('inputTopic', inputTopic);
    const concurrency = parseInt(process.argv[6]);
    console.log('concurrency', concurrency);
    // const outputTopics = JSON.parse(process.argv[7]);
    console.log('outputTopics', process.argv[7]);
    const doFunction = process.argv[8];
    console.log('doFunction', doFunction);
    const doFunctionModule = process.argv[9];
    console.log('doFunctionModule', doFunctionModule);

    const consumerGroup = name + '_ConsumerGroup';
    startDoer(name, id, brokers, inputTopic, consumerGroup, concurrency, [], doFunction, doFunctionModule);
}
launchDoer();
