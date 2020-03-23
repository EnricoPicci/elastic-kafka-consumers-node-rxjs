// npm run tsc
// node ./dist/observable-kafkajs-scripts/send-message.js -m "my message" -t myTopic -b 'localhost:9092'

import { parseCliParams } from './parse-cli-params';
import { KafkaConfig, Message, ProducerRecord, Producer } from 'kafkajs';
import { connectProducer } from '../observable-kafkajs/observable-kafkajs';
import { concatMap, tap } from 'rxjs/operators';

const example = `node ./dist/observable-kafkajs-scripts/send-message.js -m "my message" -t myTopic -b 'localhost:9092'`;
const params = parseCliParams();
if (!params.t) {
    console.error(`A topic name must be specified using the -t option like in this example ${example}`);
    process.exit(1);
}
if (!params.m) {
    console.error(`A message must be specified using the -m option like in this example ${example}`);
    process.exit(1);
}
if (!params.b) {
    console.error(`A broker must be specified using the -b option like in this example ${example}`);
    process.exit(1);
}

const clientId = 'SendMsgScript';
let producer: Producer;
const kafkaConfig: KafkaConfig = {
    clientId,
    brokers: [params.b],
};
connectProducer(kafkaConfig)
    .pipe(
        concatMap(() => connectProducer(kafkaConfig)),
        tap(_producer => (producer = _producer)),
        concatMap(producer => {
            const messages: Message[] = [
                {
                    value: params.m,
                },
            ];
            const producerRecord: ProducerRecord = {
                messages,
                topic: params.t,
            };
            return producer.send(producerRecord);
        }),
    )
    .subscribe({
        next: resp => console.log('Message sent', resp),
        error: err => {
            if (producer) {
                producer.disconnect();
            }
            console.error('ERROR', err);
        },
        complete: () => {
            console.log('DONE');
            producer.disconnect().then(
                () => console.log('Producer Disconnected'),
                err => console.error('Error while disconnecting the Producer', err),
            );
            process.exit(0);
        },
    });
