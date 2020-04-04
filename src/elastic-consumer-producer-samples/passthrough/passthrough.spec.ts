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
import { Passthrough } from './passthrough';
import { LogConsumer } from '../../elastic-consumer-samples/log-consumer/log-consumer';

describe(`when a Passthrough subscribes to a Topic`, () => {
    let adminClient: Admin;
    let initialProducer: Producer;
    let finalConsumer: LogConsumer;

    const topicNameForInitialProducer = testConfiguration.topicName + '_InitialProducer_' + Date.now().toString();
    const topicNamesForPassthroughProducer = [
        testConfiguration.topicName + '_PassthroughProducer_' + Date.now().toString(),
    ];

    before(`create the Topics, the Producer and the FinalConsumer`, (done) => {
        const kafkaConfig: KafkaConfig = {
            brokers: testConfiguration.brokers,
            retry: {
                initialRetryTime: 100,
                retries: 3,
            },
        };
        const initialProducerClientId = 'Producer';
        const initialProducerKafkaConfig: KafkaConfig = { ...kafkaConfig, clientId: initialProducerClientId };

        finalConsumer = new LogConsumer(
            'FinalConsumer',
            0,
            testConfiguration.brokers,
            topicNamesForPassthroughProducer[0],
            'TestFinalConsumerGroup',
        );
        const topicsForInitialProducer: ITopicConfig[] = [
            {
                topic: topicNameForInitialProducer,
            },
        ];
        const topicsForPassthroughProducer: ITopicConfig[] = topicNamesForPassthroughProducer.map((topic) => ({
            topic,
        }));
        const topics = [...topicsForInitialProducer, ...topicsForPassthroughProducer];
        connectAdminClient(kafkaConfig)
            .pipe(
                tap((_adminClient) => (adminClient = _adminClient)),
                concatMap(() => createTopics(adminClient, topics)),
                tap(() => adminClient.disconnect()),
                concatMap(() => connectProducer(initialProducerKafkaConfig)),
                tap((_producer) => (initialProducer = _producer)),
                tap(() => done()),
            )
            .subscribe({
                error: (err) => {
                    if (adminClient) {
                        adminClient.disconnect();
                    }
                    if (initialProducer) {
                        initialProducer.disconnect();
                    }
                    console.error('ERROR', err);
                    done(err);
                },
            });
    });
    after(`disconnects the InitialProducer, the PassthroughConsumerProducer and the FinalConsumer`, (done) => {
        Promise.all([initialProducer.disconnect(), finalConsumer.disconnect()]).then(
            () => done(),
            (err) => done(err),
        );
    });
    it(`it passes the messages received to the topics for which it is a Producer`, (done) => {
        const messageValue = 'The value of message for a Passthrough ' + Date.now().toString();
        const messages: Message[] = [
            {
                value: messageValue,
            },
        ];
        const initialProducerRecord: ProducerRecord = {
            messages,
            topic: topicNameForInitialProducer,
        };
        // Create the passthrough consumer-producer
        const passthrough = new Passthrough(
            'My Test Passthrough Consumer-Producer',
            0,
            testConfiguration.brokers,
            topicNameForInitialProducer,
            testConfiguration.groupId,
            topicNamesForPassthroughProducer,
        );
        let passthroughLoggedMessage: string;
        passthrough.logger = (message) => (passthroughLoggedMessage = message.value.toString());
        passthrough.start();

        let finalConsumerLoggedMessage: string;
        finalConsumer.logger = (message) => (finalConsumerLoggedMessage = message.value.toString());
        // The Producer sends a record
        sendRecord(initialProducer, initialProducerRecord)
            .pipe(
                // The final consumer starts consuming
                concatMap(() => finalConsumer.consume()),
                tap(() => {
                    expect(passthroughLoggedMessage).to.equal(messageValue);
                    expect(finalConsumerLoggedMessage).to.equal(messageValue);
                }),
                take(1), // to complete the Observable
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    initialProducer.disconnect();
                    passthrough.disconnect();
                    finalConsumer.disconnect(), done(err);
                },
                complete: () => {
                    initialProducer.disconnect();
                    passthrough.disconnect();
                    finalConsumer.disconnect(), done();
                },
            });
    }).timeout(60000);
});
