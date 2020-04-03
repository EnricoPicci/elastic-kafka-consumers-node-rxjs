import 'mocha';
import { expect } from 'chai';
import { KafkaConfig, ITopicConfig, Admin, Producer, Consumer, Message, ProducerRecord } from 'kafkajs';
import { testConfiguration } from '../../observable-kafkajs/test-config';
import {
    connectAdminClient,
    createTopics,
    connectProducer,
    sendRecord,
} from '../../observable-kafkajs/observable-kafkajs';
import { tap, concatMap, take } from 'rxjs/operators';
import { LogConsumer } from './log-consumer';

describe(`when a LogConsumer subscribes to a Topic`, () => {
    let adminClient: Admin;
    let topicForLogConsumer: string;
    let producer: Producer;
    before(`create the Topic, the Producer and the Consumer`, done => {
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
                tap(_adminClient => (adminClient = _adminClient)),
                concatMap(() => createTopics(adminClient, topics)),
                tap(() => adminClient.disconnect()),
                concatMap(() => connectProducer(kafkaConfig)),
                tap(_producer => (producer = _producer)),
                tap(() => done()),
            )
            .subscribe({
                error: err => {
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
    after(`disconnects the Producer and the Consumer`, done => {
        producer.disconnect().then(
            () => done(),
            err => done(err),
        );
    });
    it(`it logs any message a Producer sends to the topic`, done => {
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
            testConfiguration.brokers,
            topicForLogConsumer,
            testConfiguration.groupId,
        );
        // The Producer sends a record
        sendRecord(producer, producerRecord)
            .pipe(
                // The consumer starts
                concatMap(() => logConsumer.start()),
                // tap(msg => expect(msg.kafkaMessage.value.toString()).to.equal(messageValue)),
                take(1), // to complete the Observable
            )
            .subscribe({
                error: err => {
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
