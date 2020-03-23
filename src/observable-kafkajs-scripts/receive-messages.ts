// npm run tsc
// node ./dist/observable-kafkajs-scripts/receive-messages.js -t myTopic -b 'localhost:9092' -g myGroupId
// node ./dist/observable-kafkajs-scripts/receive-messages.js -t myTopic -b 'localhost:9092'

import { parseCliParams } from './parse-cli-params';
import { Producer, KafkaConfig, Consumer } from 'kafkajs';
import { connectConsumer, consumerMessages, subscribeConsumerToTopic } from '../observable-kafkajs/observable-kafkajs';
import { tap, concatMap } from 'rxjs/operators';
import { SignalConstants } from 'os';

const example = `node ./dist/observable-kafkajs-scripts/receive-messages.js -t myTopic -b 'localhost:9092' -g myGroupId`;
const params = parseCliParams();
if (!params.t) {
    console.error(`A topic name must be specified using the -t option like in this example ${example}`);
    process.exit(1);
}
let groupId = 'defaultConsumerGroup';
if (!params.g) {
    console.log(`The default consumer group ${groupId} is used`);
} else {
    groupId = params.g;
}
if (!params.b) {
    console.error(`A broker must be specified using the -b option like in this example ${example}`);
    process.exit(1);
}
const clientId = 'ReceiveMsgsScript';
let consumer: Consumer;
const kafkaConfig: KafkaConfig = {
    clientId,
    brokers: [params.b],
};

connectConsumer(kafkaConfig, groupId)
    .pipe(
        tap(_consumer => (consumer = _consumer)),
        concatMap(() => subscribeConsumerToTopic(consumer, params.t)),
        concatMap(() => consumerMessages(consumer)),
    )
    .subscribe({
        next: msg => {
            msg.done();
            console.log('Message received', msg);
        },
        error: err => {
            if (consumer) {
                consumer.disconnect();
            }
            console.error('ERROR', err);
        },
        complete: () => {
            console.log('DONE');
            consumer.disconnect().then(
                () => console.log('Consumer Disconnected'),
                err => console.error('Error while disconnecting the Consumer', err),
            );
            process.exit(0);
        },
    });

const errorTypes: any[] = ['unhandledRejection', 'uncaughtException'];
errorTypes.map(type => {
    process.on(type, async e => {
        try {
            console.log(`process.on ${type}`);
            console.error(e);
            await consumer.disconnect();
            process.exit(0);
        } catch (_) {
            process.exit(1);
        }
    });
});

const signalTraps: NodeJS.Signals[] = ['SIGTERM', 'SIGINT', 'SIGUSR2'];
signalTraps.map(type => {
    process.once(type, async () => {
        try {
            await consumer.disconnect();
        } finally {
            process.kill(process.pid, type);
        }
    });
});
