import 'mocha';
import { expect } from 'chai';
import { KafkaConfig, logLevel, Admin, ITopicConfig } from 'kafkajs';
import { testConfiguration } from './test-config';
import { connectAdminClient, existingTopics, disconnectAdminClient, createTopics } from './observable-kafkajs';
import { tap, concatMap } from 'rxjs/operators';

describe(`connectAdminClient function`, () => {
    it(`creates an admin client when passed valid brokers`, done => {
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
                tap(adminClient => {
                    expect(adminClient).to.be.not.undefined;
                    adminClient.disconnect();
                    done();
                }),
            )
            .subscribe({
                error: err => {
                    console.error('ERROR', err);
                    done(err);
                },
            });
    });
    it(`errors when passed invalid brokers`, done => {
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
            next: data => {
                console.error('It should not pass here', data);
                done('It should not next');
            },
            error: err => {
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
    it(`disconnects a connected client`, done => {
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
                tap(_adminClient => (adminClient = _adminClient)),
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
                next: data => {
                    console.error('It should not pass here', data);
                    done('It should not next');
                },
                error: err => {
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

describe(`createTopics function`, () => {
    it(`disconnects a connected client`, done => {
        const newTopicName = Date.now().toString();
        const topics: ITopicConfig[] = [
            {
                topic: newTopicName,
            },
        ];
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
                tap(_adminClient => (adminClient = _adminClient)),
                concatMap(() => createTopics(adminClient, topics)),
                tap(topicsCreated => {
                    expect(topicsCreated.length).to.equal(topics.length);
                    expect(topicsCreated.find(t => t === topics[0].topic)).to.be.not.undefined;
                }),
                concatMap(() => disconnectAdminClient(adminClient)),
            )
            .subscribe({
                error: err => {
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

describe(`existingTopics function`, () => {
    it(`returns an empty array since none of the topics actually exists`, done => {
        const topicNames = ['nonexiastingtopic1', 'nonexiastingtopic1'];
        let adminClient: Admin;

        const kafkaConfig: KafkaConfig = {
            clientId: 'existingTopics-test',
            brokers: testConfiguration.brokers,
            retry: {
                initialRetryTime: 100,
                retries: 3,
            },
        };
        connectAdminClient(kafkaConfig)
            .pipe(
                tap(_adminClient => (adminClient = _adminClient)),
                concatMap(() => existingTopics(adminClient, topicNames)),
                tap(topics => {
                    expect(topics.length).to.equal(0);
                    adminClient.disconnect();
                    done();
                }),
            )
            .subscribe({
                error: err => {
                    if (adminClient) {
                        adminClient.disconnect();
                    }
                    console.error('ERROR', err);
                    done(err);
                },
            });
    });
});
