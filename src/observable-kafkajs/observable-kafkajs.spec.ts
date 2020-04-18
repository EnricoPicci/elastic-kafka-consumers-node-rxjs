import 'mocha';
import { expect } from 'chai';
import { KafkaConfig, logLevel, Admin, ITopicConfig, Producer, ProducerRecord, Message, Consumer } from 'kafkajs';
import { testConfiguration } from './test-config';
import {
    connectAdminClient,
    existingTopics,
    disconnectAdminClient,
    createTopics,
    nonExistingTopics,
    fetchTopicMetadata,
    deleteTopics,
    connectConsumer,
    connectProducer,
    subscribeConsumerToTopic,
    consumerMessages,
    sendRecord,
} from './observable-kafkajs';
import { tap, concatMap, take } from 'rxjs/operators';
import { before, it, after } from 'mocha';

function _buildAdminClient(clientId: string, done: (err?) => void, result: { adminClient: Admin }) {
    const kafkaConfig: KafkaConfig = {
        clientId,
        brokers: testConfiguration.brokers,
        retry: {
            initialRetryTime: 100,
            retries: 3,
        },
    };
    connectAdminClient(kafkaConfig)
        .pipe(
            tap((_adminClient) => (result.adminClient = _adminClient)),
            tap(() => done()),
        )
        .subscribe({
            error: (err) => {
                if (result.adminClient) {
                    result.adminClient.disconnect();
                }
                console.error('ERROR', err);
                done(err);
            },
        });
}
function _disconnectAdminClient(adminClient: Admin, done: (err?) => void) {
    disconnectAdminClient(adminClient).subscribe({
        error: (err) => {
            if (adminClient) {
                adminClient.disconnect();
            }
            console.error('ERROR', err);
            done(err);
        },
        complete: () => done(),
    });
}

describe(`connectAdminClient function`, () => {
    it(`creates an admin client when passed valid brokers`, (done) => {
        const kafkaConfig: KafkaConfig = {
            clientId: 'connectAdminClient-test',
            brokers: testConfiguration.brokers,
            retry: {
                initialRetryTime: 100,
                retries: 3,
            },
        };

        connectAdminClient(kafkaConfig)
            .pipe(
                tap((adminClient) => {
                    expect(adminClient).to.be.not.undefined;
                    adminClient.disconnect();
                    done();
                }),
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    done(err);
                },
            });
    });
    it(`errors when passed invalid brokers`, (done) => {
        const kafkaConfig: KafkaConfig = {
            clientId: 'connectAdminClient-test',
            brokers: ['invalidhost:9999'],
            retry: {
                initialRetryTime: 100,
                retries: 0,
            },
            logLevel: logLevel.NOTHING,
        };

        connectAdminClient(kafkaConfig).subscribe({
            next: (data) => {
                console.error('It should not pass here', data);
                done('It should not next');
            },
            error: (err) => {
                expect(err).to.be.not.undefined;
                done();
            },
            complete: () => {
                console.error('It should not complete since it should error');
                done('It should not complete');
            },
        });
    });
});

describe(`disconnectAdminClient function`, () => {
    it(`disconnects a connected client`, (done) => {
        let adminClient: Admin;

        const kafkaConfig: KafkaConfig = {
            clientId: 'disconnectAdminClient-test',
            brokers: testConfiguration.brokers,
            retry: {
                initialRetryTime: 100,
                retries: 3,
            },
        };

        connectAdminClient(kafkaConfig)
            .pipe(
                tap((_adminClient) => {
                    adminClient = _adminClient;
                    (adminClient.logger() as any).setLogLevel(logLevel.NOTHING);
                }),
                concatMap(() => disconnectAdminClient(adminClient)),
                concatMap(() => {
                    const topics: ITopicConfig[] = [
                        {
                            topic: 'anyTopic',
                        },
                    ];
                    return createTopics(adminClient, topics);
                }),
            )
            .subscribe({
                next: (data) => {
                    console.error('It should not pass here', data);
                    done('It should not next');
                },
                error: (err) => {
                    expect(err).to.be.not.undefined;
                    expect(err.message).to.equal('Closed connection');
                    done();
                },
                complete: () => {
                    console.error('It should not complete since it should error');
                    done('It should not complete');
                },
            });
    });
});

describe(`createTopics function`, () => {
    const connectionData = { adminClient: null };
    before(`create the admin client`, (done) => {
        _buildAdminClient('createTopics-test', done, connectionData);
    });
    after(`disconnect the admin client`, (done) => {
        _disconnectAdminClient(connectionData.adminClient, done);
    });
    it(`creates a new topic`, (done) => {
        const adminClient = connectionData.adminClient;
        const newTopicName = 'TestTopic' + Date.now().toString();
        const topics: ITopicConfig[] = [
            {
                topic: newTopicName,
            },
        ];

        createTopics(adminClient, topics)
            .pipe(
                tap((topicsCreated) => {
                    expect(topicsCreated.length).to.equal(topics.length);
                    expect(topicsCreated.find((t) => t === topics[0].topic)).to.be.not.undefined;
                }),
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    done(err);
                },
                complete: () => done(),
            });
    });
    it(`creates more than one topic`, (done) => {
        const adminClient = connectionData.adminClient;
        const newTopicName_1 = 'TestTopic_1_' + Date.now().toString();
        const newTopicName_2 = 'TestTopic_2_' + Date.now().toString();
        const topics: ITopicConfig[] = [
            {
                topic: newTopicName_1,
            },
            {
                topic: newTopicName_2,
            },
        ];

        createTopics(adminClient, topics)
            .pipe(
                tap((topicsCreated) => {
                    expect(topicsCreated.length).to.equal(topics.length);
                    topics.forEach((topicToBeCreated) => {
                        expect(topicsCreated.find((t) => t === topicToBeCreated.topic)).to.be.not.undefined;
                    });
                }),
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    done(err);
                },
                complete: () => done(),
            });
    });
});

describe(`existingTopics function`, () => {
    const connectionData = { adminClient: null };
    before(`create the admin client`, (done) => {
        _buildAdminClient('existingTopics-test', done, connectionData);
    });
    after(`disconnect the admin client`, (done) => {
        _disconnectAdminClient(connectionData.adminClient, done);
    });
    it(`returns an empty array since none of the topics actually exists`, (done) => {
        const topicNames = ['nonexiastingtopic1', 'nonexiastingtopic1'];
        const adminClient = connectionData.adminClient;
        existingTopics(adminClient, topicNames)
            .pipe(
                tap((topics) => {
                    expect(topics.length).to.equal(0);
                }),
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    done(err);
                },
                complete: () => done(),
            });
    });
    it(`returns an array containing only the topics that exist`, (done) => {
        const newTopicName = 'TestTopic' + Date.now().toString();
        const topicsToBeCreated: ITopicConfig[] = [
            {
                topic: newTopicName,
            },
        ];
        const topicNames = ['nonexiastingtopic', newTopicName];
        const adminClient = connectionData.adminClient;
        createTopics(adminClient, topicsToBeCreated)
            .pipe(
                concatMap(() => existingTopics(adminClient, topicNames)),
                tap((topics) => {
                    expect(topics.length).to.equal(topicsToBeCreated.length);
                }),
            )
            .subscribe({
                error: (err) => {
                    if (adminClient) {
                        adminClient.disconnect();
                    }
                    console.error('ERROR', err);
                    done(err);
                },
                complete: () => done(),
            });
    });
});

describe(`nonExistingTopics function`, () => {
    const connectionData = { adminClient: null };
    before(`create the admin client`, (done) => {
        _buildAdminClient('nonExistingTopics-test', done, connectionData);
    });
    after(`disconnect the admin client`, (done) => {
        _disconnectAdminClient(connectionData.adminClient, done);
    });
    it(`returns an array with all the names passed as input since none of the topics actually exists`, (done) => {
        const topicNames = ['nonexiastingtopic1', 'nonexiastingtopic1'];
        const adminClient = connectionData.adminClient;
        nonExistingTopics(adminClient, topicNames)
            .pipe(
                tap((topics) => {
                    expect(topics.length).to.equal(topicNames.length);
                    topicNames.forEach((tn) => {
                        expect(topics.find((t) => t === tn)).to.be.not.undefined;
                    });
                }),
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    done(err);
                },
                complete: () => done(),
            });
    });
    it(`returns an array containing only the topics that do not exist`, (done) => {
        const newTopicName = 'TestTopic' + Date.now().toString();
        const topicsToBeCreated: ITopicConfig[] = [
            {
                topic: newTopicName,
            },
        ];
        const namesOfNonExistingTopics = ['nonexiastingtopic_1', 'nonexiastingtopic_1'];
        const topicNames = [...namesOfNonExistingTopics, newTopicName];
        const adminClient = connectionData.adminClient;
        createTopics(adminClient, topicsToBeCreated)
            .pipe(
                concatMap(() => nonExistingTopics(adminClient, topicNames)),
                tap((topics) => {
                    expect(topics.length).to.equal(namesOfNonExistingTopics.length);
                    namesOfNonExistingTopics.forEach((tn) => {
                        expect(topics.find((t) => t === tn)).to.be.not.undefined;
                    });
                }),
            )
            .subscribe({
                error: (err) => {
                    if (adminClient) {
                        adminClient.disconnect();
                    }
                    console.error('ERROR', err);
                    done(err);
                },
                complete: () => done(),
            });
    });
});

describe(`fetchTopicMetadata function`, () => {
    const connectionData: { adminClient: Admin } = { adminClient: null };
    before(`create the admin client`, (done) => {
        _buildAdminClient('fetchTopicMetadata-test', done, connectionData);
    });
    after(`disconnect the admin client`, (done) => {
        _disconnectAdminClient(connectionData.adminClient, done);
    });
    it(`returns an array of the metadata for the topic names passed as parameters`, (done) => {
        const adminClient = connectionData.adminClient;
        const newTopicName_1 = 'TestTopic_1_' + Date.now().toString();
        const newTopicName_2 = 'TestTopic_2_' + Date.now().toString();
        const topicNames = [newTopicName_1, newTopicName_2];
        const topicsToBeCreated: ITopicConfig[] = topicNames.map((tn) => ({ topic: tn }));
        createTopics(adminClient, topicsToBeCreated)
            .pipe(
                concatMap(() => fetchTopicMetadata(adminClient, topicNames)),
                tap((topicsMetadata) => {
                    expect(topicsMetadata.topics.length).to.equal(topicsToBeCreated.length);
                    topicsMetadata.topics.forEach((tMd) => expect(topicNames.includes(tMd.name)).to.be.true);
                }),
            )
            .subscribe({
                error: (err) => {
                    if (adminClient) {
                        adminClient.disconnect();
                    }
                    console.error('ERROR', err);
                    done(err);
                },
                complete: () => done(),
            });
    });
    it(`errors since none of the topic does not exist`, (done) => {
        const topicNames = ['nonexiastingtopic_1', 'nonexiastingtopic_2'];
        const adminClient = connectionData.adminClient;
        (adminClient.logger() as any).setLogLevel(logLevel.NOTHING);
        fetchTopicMetadata(adminClient, topicNames).subscribe({
            next: (data) => {
                console.error('It should not pass here', data);
                done('It should not next');
            },
            error: (err) => {
                expect(err).to.be.not.undefined;
                expect(err.message).to.equal('This server does not host this topic-partition');
                done();
            },
            complete: () => {
                console.error('It should not complete since it should error');
                done('It should not complete');
            },
        });
    });
    it(`errors since at least one of the topic does not exist`, (done) => {
        const adminClient = connectionData.adminClient;
        const newTopicName_1 = 'TestTopic_1_' + Date.now().toString();
        const newTopicName_2 = 'TestTopic_2_' + Date.now().toString();
        const topicNames = [newTopicName_1, newTopicName_2];
        const topicsToBeCreated: ITopicConfig[] = topicNames.map((tn) => ({ topic: tn }));
        (adminClient.logger() as any).setLogLevel(logLevel.NOTHING);

        createTopics(adminClient, topicsToBeCreated)
            .pipe(concatMap(() => fetchTopicMetadata(adminClient, [...topicNames, 'nonexistingtopic'])))
            .subscribe({
                next: (data) => {
                    console.error('It should not pass here', data);
                    done('It should not next');
                },
                error: (err) => {
                    expect(err).to.be.not.undefined;
                    expect(err.message).to.equal('This server does not host this topic-partition');
                    done();
                },
                complete: () => {
                    console.error('It should not complete since it should error');
                    done('It should not complete');
                },
            });
    });
});

describe(`deleteTopics function`, () => {
    const connectionData: { adminClient: Admin } = { adminClient: null };
    before(`create the admin client`, (done) => {
        _buildAdminClient('deleteTopics-test', done, connectionData);
    });
    after(`disconnect the admin client`, (done) => {
        _disconnectAdminClient(connectionData.adminClient, done);
    });
    it(`returns the name of the topic deleted`, (done) => {
        const adminClient = connectionData.adminClient;
        const topicName = 'TestTopicToDelete' + Date.now().toString();
        const topics: ITopicConfig[] = [
            {
                topic: topicName,
            },
        ];

        createTopics(adminClient, topics)
            .pipe(
                concatMap(() =>
                    deleteTopics(
                        adminClient,
                        topics.map((t) => t.topic),
                    ),
                ),
                tap((topicsDeleted) => {
                    expect(topicsDeleted.length).to.equal(topics.length);
                    expect(topicsDeleted.find((t) => t === topics[0].topic)).to.equal(topicName);
                }),
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    done(err);
                },
                complete: () => done(),
            });
    });
    it(`returns an empty array since the topic to be deleted does not exist`, (done) => {
        const adminClient = connectionData.adminClient;
        const topics = ['NonExistingTopicToDelete'];

        deleteTopics(adminClient, topics)
            .pipe(
                tap((topicsDeleted) => {
                    expect(topicsDeleted.length).to.equal(0);
                }),
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    done(err);
                },
                complete: () => done(),
            });
    });
    it(`returns an array with just the names of the topics actually deleted`, (done) => {
        const adminClient = connectionData.adminClient;
        const topicName = 'TestTopicToDelete' + Date.now().toString();
        const topicsToDelete: ITopicConfig[] = [
            {
                topic: topicName,
            },
        ];
        const topics = [...topicsToDelete.map((t) => t.topic), 'NonExistingTopic'];

        // just some of the topics, i.e. the topics to delete, are created
        createTopics(adminClient, topicsToDelete)
            .pipe(
                concatMap(() => deleteTopics(adminClient, topics)),
                tap((topicsDeleted) => {
                    expect(topicsDeleted.length).to.equal(topicsToDelete.length);
                    expect(topicsDeleted.find((t) => t === topicName)).to.be.not.undefined;
                }),
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    done(err);
                },
                complete: () => done(),
            });
    });
    it(`actually deletes a topic`, (done) => {
        const adminClient = connectionData.adminClient;
        const topicName = 'TestTopicCreatedAndThenDeleted' + Date.now().toString();
        const topics: ITopicConfig[] = [
            {
                topic: topicName,
            },
        ];
        (adminClient.logger() as any).setLogLevel(logLevel.NOTHING);

        createTopics(adminClient, topics)
            .pipe(
                // first test that the topic exists
                concatMap(() => fetchTopicMetadata(adminClient, [topicName])),
                tap((topicsMetadata) => {
                    expect(topicsMetadata.topics.length).to.equal(1);
                }),
                concatMap(() =>
                    deleteTopics(
                        adminClient,
                        topics.map((t) => t.topic),
                    ),
                ),
                // then test that the topic does not exist - this raises an error
                concatMap(() => fetchTopicMetadata(adminClient, [topicName])),
            )
            .subscribe({
                next: (data) => {
                    console.error('It should not pass here', data);
                    done('It should not next');
                },
                error: (err) => {
                    expect(err).to.be.not.undefined;
                    expect(err.message).to.equal('This server does not host this topic-partition');
                    done();
                },
                complete: () => {
                    console.error('It should not complete since it should error');
                    done('It should not complete');
                },
            });
    });
});

describe(`connectConsumer function`, () => {
    it(`returns a Consumer connected to Kafka brokers`, (done) => {
        const kafkaConfig: KafkaConfig = {
            brokers: testConfiguration.brokers,
        };

        connectConsumer(kafkaConfig, testConfiguration.groupId)
            .pipe(
                tap((consumer) => {
                    expect(consumer).to.be.not.undefined;
                }),
                tap((consumer) => {
                    consumer.disconnect();
                }),
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    done(err);
                },
                complete: () => done(),
            });
    });
});

describe(`connectProducer function`, () => {
    it(`returns a Producer connected to Kafka brokers`, (done) => {
        const kafkaConfig: KafkaConfig = {
            brokers: testConfiguration.brokers,
        };

        connectProducer(kafkaConfig)
            .pipe(
                tap((producer) => {
                    expect(producer).to.be.not.undefined;
                }),
                tap((producer) => {
                    producer.disconnect();
                }),
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    done(err);
                },
                complete: () => done(),
            });
    });
});

describe(`when a Producer sends a record to a Topic`, () => {
    let adminClient: Admin;
    let newTopicName: string;
    let producer: Producer;
    let consumer: Consumer;
    before(`create the Topic, the Producer and the Consumer`, (done) => {
        const clientId = 'ProducerSendMsg';
        const kafkaConfig: KafkaConfig = {
            clientId,
            brokers: testConfiguration.brokers,
            retry: {
                initialRetryTime: 100,
                retries: 3,
            },
        };
        newTopicName = testConfiguration.topicName + '_ProducerFirst_' + Date.now().toString();
        const topics: ITopicConfig[] = [
            {
                topic: newTopicName,
            },
        ];
        connectAdminClient(kafkaConfig)
            .pipe(
                tap((_adminClient) => (adminClient = _adminClient)),
                concatMap(() => createTopics(adminClient, topics)),
                tap(() => adminClient.disconnect()),
                concatMap(() => connectProducer(kafkaConfig)),
                tap((_producer) => (producer = _producer)),
                concatMap(() => connectConsumer(kafkaConfig, testConfiguration.groupId)),
                tap((_consumer) => (consumer = _consumer)),
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
    after(`disconnects the Producer and the Consumer`, (done) => {
        producer
            .disconnect()
            .then(() => consumer.disconnect())
            .then(
                () => done(),
                (err) => done(err),
            );
    });
    it(`a Consumer reads the record even if it subscribes to the topic AFTER the record has been sent`, (done) => {
        const messageValue = 'The value of message Producer first ' + Date.now().toString();
        const messages: Message[] = [
            {
                value: messageValue,
            },
        ];
        const producerRecord: ProducerRecord = {
            messages,
            topic: newTopicName,
        };
        // First the Producer sends a message, while the Consumer is not yet subscribed
        sendRecord(producer, producerRecord)
            .pipe(
                // Then subscribe the Consumer to the topic
                concatMap(() => subscribeConsumerToTopic(consumer, newTopicName)),
                concatMap(() => consumerMessages(consumer)),
                tap((msg) => expect(msg.kafkaMessage.value.toString()).to.equal(messageValue)),
                take(1), // to complete the Observable
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    producer.disconnect();
                    consumer.disconnect();
                    done(err);
                },
                complete: () => {
                    producer.disconnect();
                    consumer.disconnect();
                    done();
                },
            });
    }).timeout(30000);
});

describe(`when a Consumer subscribes to a Topic`, () => {
    let adminClient: Admin;
    let newTopicName: string;
    let producer: Producer;
    let consumer: Consumer;
    before(`create the Topic, the Producer and the Consumer`, (done) => {
        const clientId = 'ConsumerReadMsg';
        const kafkaConfig: KafkaConfig = {
            clientId,
            brokers: testConfiguration.brokers,
            retry: {
                initialRetryTime: 100,
                retries: 3,
            },
        };
        newTopicName = testConfiguration.topicName + '_ConsumerFirst_' + Date.now().toString();
        const topics: ITopicConfig[] = [
            {
                topic: newTopicName,
            },
        ];
        connectAdminClient(kafkaConfig)
            .pipe(
                tap((_adminClient) => (adminClient = _adminClient)),
                concatMap(() => createTopics(adminClient, topics)),
                tap(() => adminClient.disconnect()),
                concatMap(() => connectProducer(kafkaConfig)),
                tap((_producer) => (producer = _producer)),
                concatMap(() => connectConsumer(kafkaConfig, testConfiguration.groupId)),
                tap((_consumer) => (consumer = _consumer)),
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
    after(`disconnects the Producer and the Consumer`, (done) => {
        producer
            .disconnect()
            .then(() => consumer.disconnect())
            .then(
                () => done(),
                (err) => done(err),
            );
    });
    it(`a Producer can send a record to the topic and the Consumer reads it`, (done) => {
        const messageValue = 'The value of message Consumer first ' + Date.now().toString();
        const messages: Message[] = [
            {
                value: messageValue,
            },
        ];
        const producerRecord: ProducerRecord = {
            messages,
            topic: newTopicName,
        };
        // First the Consumer subscribed to the topic
        subscribeConsumerToTopic(consumer, newTopicName)
            .pipe(
                // Then the Producer sends a record, while the Consumer is ALREADY subscribed
                concatMap(() => sendRecord(producer, producerRecord)),
                concatMap(() => consumerMessages(consumer)),
                tap((msg) => expect(msg.kafkaMessage.value.toString()).to.equal(messageValue)),
                take(1), // to complete the Observable
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    producer.disconnect();
                    consumer.disconnect();
                    done(err);
                },
                complete: () => {
                    producer.disconnect();
                    consumer.disconnect();
                    done();
                },
            });
    }).timeout(60000);
});
