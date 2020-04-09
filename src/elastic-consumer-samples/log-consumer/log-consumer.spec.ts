import 'mocha';
import { expect } from 'chai';
import { KafkaConfig, ITopicConfig, Admin, Producer, Message, ProducerRecord } from 'kafkajs';
import { testConfiguration } from '../../observable-kafkajs/test-config';
import {
    connectAdminClient,
    createTopics,
    connectProducer,
    sendRecord,
} from '../../observable-kafkajs/observable-kafkajs';
import { tap, concatMap, take, toArray } from 'rxjs/operators';
import { LogConsumer } from './log-consumer';
import { of } from 'rxjs';

describe(`when a LogConsumer subscribes to a Topic`, () => {
    let adminClient: Admin;
    let topicForLogConsumer: string;
    let producer: Producer;
    before(`create the Topic and the Producer`, (done) => {
        const clientId = 'LogConsumer';
        const kafkaConfig: KafkaConfig = {
            clientId,
            brokers: testConfiguration.brokers,
            retry: {
                initialRetryTime: 100,
                retries: 3,
            },
        };
        topicForLogConsumer = testConfiguration.topicName + '_LogConsumer_' + Date.now().toString();
        const topics: ITopicConfig[] = [
            {
                topic: topicForLogConsumer,
            },
        ];
        connectAdminClient(kafkaConfig)
            .pipe(
                tap((_adminClient) => (adminClient = _adminClient)),
                concatMap(() => createTopics(adminClient, topics)),
                tap(() => adminClient.disconnect()),
                concatMap(() => connectProducer(kafkaConfig)),
                tap((_producer) => (producer = _producer)),
                tap(() => done()),
            )
            .subscribe({
                error: (err) => {
                    if (adminClient) {
                        adminClient.disconnect();
                    }
                    if (producer) {
                        producer.disconnect();
                    }
                    console.error('ERROR', err);
                    done(err);
                },
            });
    });
    after(`disconnects the Producer`, (done) => {
        producer.disconnect().then(
            () => done(),
            (err) => done(err),
        );
    });

    describe(`and a Producer sends one message to that Topic`, () => {
        it(`it logs that message`, (done) => {
            const messageValue = 'The value of message for a LogConsumer ' + Date.now().toString();
            const messages: Message[] = [
                {
                    value: messageValue,
                },
            ];
            const producerRecord: ProducerRecord = {
                messages,
                topic: topicForLogConsumer,
            };
            // Create the log consumer
            const logConsumer = new LogConsumer(
                'My Test Log Consumer',
                0,
                testConfiguration.brokers,
                topicForLogConsumer,
                testConfiguration.groupId,
            );
            let loggedMessage: string;
            logConsumer.logger = (message) => {
                loggedMessage = message.value.toString();
                return of(`${message.value} logged`);
            };
            // The Producer sends a record
            sendRecord(producer, producerRecord)
                .pipe(
                    // The consumer starts
                    concatMap(() => logConsumer.consume()),
                    tap(() => expect(loggedMessage).to.equal(messageValue)),
                    take(1), // to complete the Observable
                )
                .subscribe({
                    error: (err) => {
                        console.error('ERROR', err);
                        producer.disconnect();
                        logConsumer.disconnect();
                        done(err);
                    },
                    complete: () => {
                        producer.disconnect();
                        logConsumer.disconnect();
                        done();
                    },
                });
        }).timeout(60000);
    });

    describe(`and a Producer sends MORE than one message to that Topic`, () => {
        it(`it logs all the messages`, (done) => {
            const messageValue1 = 'Message_1 value for a LogConsumer ' + Date.now().toString();
            const messageValue2 = 'Message_2 value for a LogConsumer ' + Date.now().toString();
            const messageValue3 = 'Message_3 value for a LogConsumer ' + Date.now().toString();
            const messageValues = [messageValue1, messageValue2, messageValue3];
            const messages = messageValues.map((value) => ({
                value,
            }));
            const producerRecord: ProducerRecord = {
                messages,
                topic: topicForLogConsumer,
            };
            // Create the log consumer
            const logConsumer = new LogConsumer(
                'My Test Log Consumer',
                0,
                testConfiguration.brokers,
                topicForLogConsumer,
                testConfiguration.groupId,
            );
            let loggedMessages: string[] = [];
            logConsumer.logger = (message) => {
                loggedMessages.push(message.value.toString());
                return of(`${message.value} logged`);
            };
            // The Producer sends a record
            sendRecord(producer, producerRecord)
                .pipe(
                    // The consumer starts
                    concatMap(() => logConsumer.consume()),
                    take(messages.length), // to complete the Observable
                    toArray(),
                    tap(() => {
                        expect(loggedMessages.length).to.equal(messageValues.length);
                        messageValues.forEach((messageValue) => {
                            loggedMessages.includes(messageValue);
                        });
                    }),
                )
                .subscribe({
                    error: (err) => {
                        console.error('ERROR', err);
                        producer.disconnect();
                        logConsumer.disconnect();
                        done(err);
                    },
                    complete: () => {
                        producer.disconnect();
                        logConsumer.disconnect();
                        done();
                    },
                });
        }).timeout(60000);
    });
});
