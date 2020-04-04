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
import { tap, concatMap, take } from 'rxjs/operators';
import { FormatConsumer } from './format-consumer';

describe(`when a FormatConsumer subscribes to a Topic`, () => {
    let adminClient: Admin;
    let topicForFormatConsumer: string;
    let producer: Producer;
    before(`create the Topic, the Producer and the Consumer`, done => {
        const clientId = 'FormatConsumer';
        const kafkaConfig: KafkaConfig = {
            clientId,
            brokers: testConfiguration.brokers,
            retry: {
                initialRetryTime: 100,
                retries: 3,
            },
        };
        topicForFormatConsumer = testConfiguration.topicName + '_FormatConsumer_' + Date.now().toString();
        const topics: ITopicConfig[] = [
            {
                topic: topicForFormatConsumer,
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
    it(`it formats any message a Producer sends to the topic`, done => {
        const messageValue = 'The value of message for a FormatConsumer ' + Date.now().toString();
        const messages: Message[] = [
            {
                value: messageValue,
            },
        ];
        const producerRecord: ProducerRecord = {
            messages,
            topic: topicForFormatConsumer,
        };
        // Create the format consumer
        const formatConsumer = new FormatConsumer(
            'My Test Format Consumer',
            testConfiguration.brokers,
            topicForFormatConsumer,
            testConfiguration.groupId,
        );
        let formattedMessage: string;
        formatConsumer.formatter = message => {
            formattedMessage = message.value.toString() + ' FORMATTED';
            return formattedMessage;
        };
        // The Producer sends a record
        sendRecord(producer, producerRecord)
            .pipe(
                // The consumer starts
                concatMap(() => formatConsumer.start()),
                tap(() => expect(formattedMessage).to.equal(messageValue)),
                take(1), // to complete the Observable
            )
            .subscribe({
                error: err => {
                    console.error('ERROR', err);
                    producer.disconnect();
                    formatConsumer.disconnect();
                    done(err);
                },
                complete: () => {
                    producer.disconnect();
                    formatConsumer.disconnect();
                    done();
                },
            });
    }).timeout(60000);
});
