import 'mocha';
import { expect } from 'chai';
import { Doer, doerFuntion } from './doer';

import { testConfiguration } from '../observable-kafkajs/test-config';
import { KafkaConfig, Producer, ProducerRecord, KafkaMessage } from 'kafkajs';
import { connectProducer } from '../observable-kafkajs/observable-kafkajs';
import { concatMap, tap, take, toArray } from 'rxjs/operators';
import { of, Subject } from 'rxjs';

function kafkaConfig(clientId: string): KafkaConfig {
    return {
        clientId,
        brokers: testConfiguration.brokers,
        retry: {
            initialRetryTime: 100,
            retries: 3,
        },
    };
}

function emitFactory(emitter: Subject<string>): doerFuntion {
    return (message: KafkaMessage) => {
        emitter.next(message.value.toString());
        return of(message);
    };
}

describe(`When a message is sent to a topic which is the input topic of a Doer`, () => {
    let producer: Producer;
    let doer: Doer;
    function disconnect() {
        return producer.disconnect().then(() => doer.disconnect());
    }
    it(`the message is read and processed by the do function`, (done) => {
        const doerName = 'ConsumerDoer_' + Date.now().toString();
        const doerInputTopic = doerName + '_Topic' + Date.now().toString();
        doer = new Doer(doerName, 0, testConfiguration.brokers, doerInputTopic, doerName + '_ConsumerGroup');
        const emitter = new Subject<string>();
        doer.do = emitFactory(emitter);
        doer.start();

        const producerMessage = 'a message';
        const producerRecord: ProducerRecord = {
            messages: [
                {
                    value: producerMessage,
                },
            ],
            topic: doerInputTopic,
        };
        connectProducer(kafkaConfig(doerName))
            .pipe(
                tap((_producer) => (producer = _producer)),
                concatMap(() => producer.send(producerRecord)),
                concatMap(() => emitter),
                take(1),
                tap((val) => expect(val).to.equal(producerMessage)),
            )
            .subscribe({
                error: (e) => {
                    disconnect();
                    done(e);
                },
                complete: () => {
                    disconnect().then(
                        () => done(),
                        (err) => done(err),
                    );
                },
            });
    }).timeout(10000);
});

describe(`When a Doer has an output topic which is the input topic of another Doer`, () => {
    let producer: Producer;
    let firstDoer: Doer;
    let secondDoer: Doer;
    function disconnect() {
        return producer
            .disconnect()
            .then(() => firstDoer.disconnect())
            .then(() => secondDoer.disconnect());
    }
    function transformMessage(messageVal: string) {
        return messageVal + '_transformed';
    }
    function messageTransformFactory(): doerFuntion {
        return (message: KafkaMessage) => {
            return of(transformMessage(message.value.toString()));
        };
    }
    it(`the results of the first Doer processing are sent to the second Doer`, (done) => {
        const firstDoerName = 'ConsumerProducerFirstDoer_' + Date.now().toString();
        const firstDoerInputTopic = firstDoerName + '_Input_Topic' + Date.now().toString();
        const secondDoerName = 'ConsumerSecondDoer_' + Date.now().toString();
        const secondDoerInputTopic = secondDoerName + '_Input_Topic' + Date.now().toString();
        const firstDoerOutputTopics = [secondDoerInputTopic];
        firstDoer = new Doer(
            firstDoerName,
            0,
            testConfiguration.brokers,
            firstDoerInputTopic,
            firstDoerName + '_ConsumerGroup',
            firstDoerOutputTopics,
        );
        firstDoer.do = messageTransformFactory();
        firstDoer.start();

        secondDoer = new Doer(
            secondDoerName,
            0,
            testConfiguration.brokers,
            secondDoerInputTopic,
            secondDoerName + '_ConsumerGroup',
        );
        const emitter = new Subject<string>();
        secondDoer.do = emitFactory(emitter);
        secondDoer.start();

        const producerMessages = ['message_1', 'message_2'];
        const producerRecord: ProducerRecord = {
            messages: producerMessages.map((m) => ({
                value: m,
            })),
            topic: firstDoerInputTopic,
        };
        connectProducer(kafkaConfig(firstDoerName))
            .pipe(
                tap((_producer) => (producer = _producer)),
                concatMap(() => producer.send(producerRecord)),
                concatMap(() => emitter),
                take(producerMessages.length),
                toArray(),
                tap((receivedMessages) => {
                    expect(receivedMessages.length).to.equal(producerMessages.length);
                    producerMessages.forEach((m) => expect(receivedMessages.includes(transformMessage(m))).to.be.true);
                }),
            )
            .subscribe({
                error: (e) => {
                    disconnect();
                    done(e);
                },
                complete: () => {
                    disconnect().then(
                        () => done(),
                        (err) => done(err),
                    );
                },
            });
    }).timeout(100000);
});
