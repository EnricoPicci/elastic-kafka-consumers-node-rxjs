import { from, Observable, Subscriber, TeardownLogic } from 'rxjs';
import { map, expand, filter, delay, first, tap, concatMap } from 'rxjs/operators';

import { KafkaConfig, ITopicConfig, Kafka, Admin, Consumer, KafkaMessage, Producer, ProducerRecord } from 'kafkajs';

export const connectAdminClient = (config: KafkaConfig) => {
    const kafka = new Kafka(config);
    const admin = kafka.admin();
    return from(admin.connect()).pipe(map(() => admin));
};

export const disconnectAdminClient = (adminClient: Admin) => from(adminClient.disconnect());

export const createTopics = (adminClient: Admin, topics: ITopicConfig[]) => {
    let _topicsToCreate: ITopicConfig[];
    return existingTopics(
        adminClient,
        topics.map((t) => t.topic),
    ).pipe(
        tap((_existingTopics) => {
            if (_existingTopics.length > 0) {
                console.log(
                    `These topics "${_existingTopics.map(
                        (t) => t.name,
                    )}" do already exist and therefore are not created`,
                );
            }
        }),
        map((_existingTopics) => {
            const _existingTopicNames = _existingTopics.map((t) => t.name);
            _topicsToCreate = topics.filter((t) => !_existingTopicNames.includes(t.topic));
            return _topicsToCreate;
        }),
        concatMap((topicsToCreate) => from(adminClient.createTopics({ topics: topicsToCreate }))),
        map((topicsCreated) => {
            if (topicsCreated) {
                return _topicsToCreate.map((t) => t.topic);
            } else {
                const errorMessage =
                    topics.length === 1
                        ? `The topic ${topics[0].topic} has not been created`
                        : `Some topics have not been created`;
                throw new Error(errorMessage);
            }
        }),
    );
};

export const deleteTopics = (adminClient: Admin, topics: string[], timeout?: number) => {
    const _delay = 100;
    const maxAttempts = timeout ? Math.floor(timeout / _delay) : 10;
    let _topicsToBeDeleted: string[];
    return nonExistingTopics(adminClient, topics).pipe(
        tap((_nonExistingTopics) => {
            if (_nonExistingTopics.length > 0) {
                console.log(`These topics ${_nonExistingTopics} do not exist and therefore are not cancelled`);
            }
        }),
        map((_nonExistingTopics) => topics.filter((t) => !_nonExistingTopics.includes(t))),
        concatMap((topicsToBeDeleted) => {
            _topicsToBeDeleted = topicsToBeDeleted;
            const options = timeout ? { topics: topicsToBeDeleted, timeout } : { topics: topicsToBeDeleted };
            return from(adminClient.deleteTopics(options)).pipe(map(() => topicsToBeDeleted));
        }),
        expand((topicsToBeDeleted, index) => {
            if (index > maxAttempts) {
                throw new Error(
                    `Topics ${topicsToBeDeleted} not deleted yet after ${_delay * maxAttempts} milliseconds`,
                );
            }
            return existingTopics(adminClient, topics).pipe(delay(_delay));
        }),
        filter((_existingTopics) => {
            return _existingTopics.length === 0;
        }),
        first(),
        map(() => _topicsToBeDeleted),
    );
};

export const deleteCreateTopics = (adminClient: Admin, topics: ITopicConfig[], timeout?: number) => {
    return deleteTopics(
        adminClient,
        topics.map((t) => t.topic),
        timeout,
    ).pipe(
        tap((topicsDeleted) => console.log('Topics deleted', topicsDeleted)),
        concatMap(() => createTopics(adminClient, topics)),
    );
};

// given an array of topic names, returns the topics that exist
export const existingTopics = (adminClient: Admin, topicNames: string[]) => {
    return fetchTopicMetadata(adminClient).pipe(map(({ topics }) => topics.filter((t) => topicNames.includes(t.name))));
};
// given an array of topic names, returns the topics that DON NOT exist
export const nonExistingTopics = (adminClient: Admin, topicNames: string[]) => {
    return fetchTopicMetadata(adminClient).pipe(
        map(({ topics }) => {
            const existingTopicNames = topics.map((t) => t.name);
            return topicNames.filter((t) => !existingTopicNames.includes(t));
        }),
    );
};

export const fetchTopicMetadata = (adminClient: Admin, topics?: string[]) => {
    return from(adminClient.fetchTopicMetadata({ topics }));
};

export const fetchTopicOffsets = (adminClient: Admin, topic: string) => {
    return from(adminClient.fetchTopicOffsets(topic));
};
export const fetchOffsets = (adminClient: Admin, groupId: string, topic: string) => {
    return from(adminClient.fetchOffsets({ groupId, topic }));
};

export const connectConsumer = (config: KafkaConfig, groupId: string) => {
    const kafka = new Kafka(config);
    const consumer = kafka.consumer({ groupId });
    // consumer.on(consumer.events.CONNECT, (e) => {
    //     console.log(`consmuer connected at ${e}`);
    // });
    // return new Observable<Consumer>(
    //     (subscriber: Subscriber<Consumer>): TeardownLogic => {
    //         consumer.connect().then(
    //             () => {
    //                 console.log('consumer connected');
    //                 consumer.on(consumer.events.CONNECT, (e) => {
    //                     console.log(`consmuer connected at ${e}`);
    //                     subscriber.next(consumer);
    //                 });
    //             },
    //             (e) => console.error('error while connecting', e),
    //         );
    //     },
    // );
    return from(consumer.connect()).pipe(map(() => consumer));
};

export const subscribeConsumerToTopic = (consumer: Consumer, topic: string, fromBeginning = true) => {
    return from(consumer.subscribe({ topic, fromBeginning })).pipe(map(() => topic));
};

export const connectProducer = (config: KafkaConfig) => {
    const kafka = new Kafka(config);
    const producer = kafka.producer();
    return from(producer.connect()).pipe(map(() => producer));
};

export const sendRecord = (producer: Producer, producerRecord: ProducerRecord) => {
    return from(producer.send(producerRecord));
};

export type ConsumerMessage = {
    topic: string;
    partition: any;
    kafkaMessage: KafkaMessage;
    done: () => void;
};
export const consumerMessages = (consumer: Consumer) => {
    const rxjsSubscriberPropertyName = '_rxjs_subscriber_';
    return new Observable<ConsumerMessage>(
        (subscriber: Subscriber<ConsumerMessage>): TeardownLogic => {
            // store the rxjs subscriber as property of the Kafka consumer
            consumer[rxjsSubscriberPropertyName] = subscriber;
            // this check is to avoid calling 'run' more than once on the same Kafka consumer
            if (!consumer['running']) {
                consumer['running'] = true;
                consumer.run({
                    autoCommit: false,
                    eachMessage: async ({ topic, partition, message }) => {
                        console.log('>>>>>>>>>', message.key?.toString(), message.value?.toString());
                        const offset = (parseInt(message.offset) + 1).toString();
                        const done = () => {
                            consumer.commitOffsets([{ topic, partition, offset }]);
                        };
                        // the new rxjs subscriber, stored in the Kafka consumer, is the object whose 'next' method is called
                        consumer[rxjsSubscriberPropertyName].next({ topic, partition, kafkaMessage: message, done });
                    },
                });
            }
        },
    );
};
