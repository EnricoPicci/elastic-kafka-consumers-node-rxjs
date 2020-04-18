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
import { tap, concatMap, take, toArray, delay } from 'rxjs/operators';
import { DoConsumer } from './do-consumer';
import { of, Subscription } from 'rxjs';

describe(`when a DoConsumer subscribes to a Topic and a Producer sends one message to that Topic`, () => {
    let adminClient: Admin;
    const topicForDoConsumer = 'OneMessage_DoConsumer_' + Date.now().toString();
    let producer: Producer;
    before(`create the Topic and the Producer`, (done) => {
        const clientId = 'DoConsumer';
        const kafkaConfig: KafkaConfig = {
            clientId,
            brokers: testConfiguration.brokers,
            retry: {
                initialRetryTime: 100,
                retries: 3,
            },
        };
        const topics: ITopicConfig[] = [
            {
                topic: topicForDoConsumer,
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

    it(`it does something with that message`, (done) => {
        const messageValue = 'The value of message for a DoConsumer ' + Date.now().toString();
        const messages: Message[] = [
            {
                value: messageValue,
            },
        ];
        const producerRecord: ProducerRecord = {
            messages,
            topic: topicForDoConsumer,
        };
        // Create the do consumer
        const doConsumer = new DoConsumer(
            'My Test Do Consumer',
            0,
            testConfiguration.brokers,
            topicForDoConsumer,
            'DoConsumer_Test_ProcessOneMessage',
        );
        // the DoConsumer simply logs the message
        let loggedMessage: string;
        doConsumer.doer = (message) => {
            loggedMessage = message.value.toString();
            return of(`${message.value} logged by the DoConsumer`);
        };
        // The Producer sends a record
        sendRecord(producer, producerRecord)
            .pipe(
                // The consumer starts
                concatMap(() => doConsumer.consume()),
                tap(() => expect(loggedMessage).to.equal(messageValue)),
                take(1), // to complete the Observable
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    producer.disconnect();
                    doConsumer.disconnect();
                    done(err);
                },
                complete: () => {
                    producer.disconnect();
                    doConsumer.disconnect();
                    done();
                },
            });
    }).timeout(60000);
});

describe(`when a DoConsumer subscribes to a Topic and a Producer sends MORE than one message to that Topic`, () => {
    let adminClient: Admin;
    const topicForDoConsumer = 'ManyMessages_DoConsumer_' + Date.now().toString();
    let producer: Producer;
    before(`create the Topic and the Producer`, (done) => {
        const clientId = 'DoConsumer';
        const kafkaConfig: KafkaConfig = {
            clientId,
            brokers: testConfiguration.brokers,
            retry: {
                initialRetryTime: 100,
                retries: 3,
            },
        };
        const topics: ITopicConfig[] = [
            {
                topic: topicForDoConsumer,
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

    it(`it processes all the messages`, (done) => {
        const messageValue1 = 'Message_1 value for a DoConsumer ' + Date.now().toString();
        const messageValue2 = 'Message_2 value for a DoConsumer ' + Date.now().toString();
        const messageValue3 = 'Message_3 value for a DoConsumer ' + Date.now().toString();
        const messageValues = [messageValue1, messageValue2, messageValue3];
        const messages = messageValues.map((value) => ({
            value,
        }));
        const producerRecord: ProducerRecord = {
            messages,
            topic: topicForDoConsumer,
        };
        // Create the DoConsumer
        const doConsumer = new DoConsumer(
            'My Test Do Consumer',
            0,
            testConfiguration.brokers,
            topicForDoConsumer,
            'DoConsumer_Test_ProcessManyMessages',
        );
        // the DoConsumer simply logs the message
        let loggedMessages: string[] = [];
        doConsumer.doer = (message) => {
            loggedMessages.push(message.value.toString());
            return of(`${message.value} logged by DoConsumer`);
        };
        // The Producer sends a record
        sendRecord(producer, producerRecord)
            .pipe(
                // The consumer starts
                concatMap(() => doConsumer.consume()),
                take(messages.length), // to complete the Observable
                toArray(),
                tap(() => {
                    expect(loggedMessages.length).to.equal(messageValues.length);
                    messageValues.forEach((messageValue) => {
                        if (!loggedMessages.includes(messageValue)) {
                            console.log(messageValues, loggedMessages, messageValue);
                        }
                        expect(loggedMessages.includes(messageValue)).to.be.true;
                    });
                }),
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    producer.disconnect();
                    doConsumer.disconnect();
                    done(err);
                },
                complete: () => {
                    producer.disconnect();
                    doConsumer.disconnect();
                    done();
                },
            });
    }).timeout(60000);
});

describe(`when a DoConsumer subscribes to a Topic  and the concurrency is set to 1
and a Producer sends MORE than one message to that Topic`, () => {
    let adminClient: Admin;
    const topicForDoConsumer = 'ConcurrencyOne_DoConsumer_' + Date.now().toString();
    let producer: Producer;
    const concurrency = 1;
    before(`create the Topic and the Producer`, (done) => {
        const clientId = 'DoConsumer';
        const kafkaConfig: KafkaConfig = {
            clientId,
            brokers: testConfiguration.brokers,
            retry: {
                initialRetryTime: 100,
                retries: 3,
            },
        };
        const topics: ITopicConfig[] = [
            {
                topic: topicForDoConsumer,
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

    it(`it processes all the messages sequentially`, (done) => {
        const messageValue1 = 'Message_1_concurrency_1 value for a DoConsumer ' + Date.now().toString();
        const messageValue2 = 'Message_2_concurrency_1 value for a DoConsumer ' + Date.now().toString();
        const messageValue3 = 'Message_3_concurrency_1 value for a DoConsumer ' + Date.now().toString();
        const messageValues = [messageValue1, messageValue2, messageValue3];
        const messages = messageValues.map((value) => ({
            value,
        }));
        const producerRecord: ProducerRecord = {
            messages,
            topic: topicForDoConsumer,
        };
        // Create the DoConsumer
        const doConsumer = new DoConsumer(
            'My Test Do Consumer',
            0,
            testConfiguration.brokers,
            topicForDoConsumer,
            'DoConsumer_Test_ProcessMessagesSequentially',
            concurrency,
        );
        // In order to check whether the processing is done sequentially for every message,
        // the function pushes a message in a queue and registers the size of the queue
        // and wait for some time before removing the message and returning
        // If the processing is performed sequentially the queue size should be always 1
        let queue: string[] = [];
        let queueSize: number[] = [];
        doConsumer.doer = (message) => {
            const messagePushedInTheQueue = message.value.toString();
            queue.push(message.value.toString());
            queueSize.push(queue.length);
            return of(`${message.value} processed by DoConsumer`).pipe(
                delay(100),
                tap(() => {
                    const lastMessage = queue.pop();
                    expect(lastMessage).to.equal(messagePushedInTheQueue);
                }),
            );
        };
        // The Producer sends a record
        sendRecord(producer, producerRecord)
            .pipe(
                // The consumer starts
                concatMap(() => doConsumer.consume()),
                take(messages.length), // to complete the Observable
                toArray(),
                tap(() => {
                    expect(queueSize.length).to.equal(messageValues.length);
                    queueSize.forEach((size) => {
                        expect(size).to.equal(1);
                    });
                }),
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    producer.disconnect();
                    doConsumer.disconnect();
                    done(err);
                },
                complete: () => {
                    producer.disconnect();
                    doConsumer.disconnect();
                    done();
                },
            });
    }).timeout(60000);
});

describe(`when a DoConsumer subscribes to a Topic and the concurrency is set to greater than 1
and a Producer sends MORE than one message to that Topic`, () => {
    let adminClient: Admin;
    const topicForDoConsumer = 'ConcurrencyN_DoConsumer_' + Date.now().toString();
    let producer: Producer;
    before(`create the Topic and the Producer`, (done) => {
        const clientId = 'DoConsumer';
        const kafkaConfig: KafkaConfig = {
            clientId,
            brokers: testConfiguration.brokers,
            retry: {
                initialRetryTime: 100,
                retries: 3,
            },
        };
        const topics: ITopicConfig[] = [
            {
                topic: topicForDoConsumer,
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

    it(`it processes the messages concurrently reaching the expected level of concurrency`, (done) => {
        const concurrency = 3;
        const numberOfMessages = concurrency * 2;
        const messageValues = new Array(numberOfMessages)
            .fill(null)
            .map((_, i) => `Message_${i}_concurrency_${concurrency} value for a DoConsumer`);
        const messages = messageValues.map((value) => ({
            value,
        }));
        const producerRecord: ProducerRecord = {
            messages,
            topic: topicForDoConsumer,
        };
        // Create the DoConsumer
        const doConsumer = new DoConsumer(
            'My Test Do Consumer Processing Messages concurrently',
            0,
            testConfiguration.brokers,
            topicForDoConsumer,
            'DoConsumer_Test_ProcessMessagesConcurrently',
            concurrency,
        );
        // In order to check whether the processing is done concurrently,
        // the function pushes a message in a dictionary, registers the size of the dictionary
        // and wait for some time before removing the message and returning
        // If the processing is performed concurrently with the expected level of concurrency
        // the dictionary size at some point should reach the size of the expected concurrency
        // and should never exceed it
        let messageDictionary: { [message: string]: string } = {};
        let messageDictionarySize: number[] = [];
        doConsumer.doer = (message) => {
            const messagePushedInTheDictionary = message.value.toString();
            messageDictionary[messagePushedInTheDictionary] = messagePushedInTheDictionary;
            messageDictionarySize.push(Object.keys(messageDictionary).length);
            return of(`${messagePushedInTheDictionary} processed by DoConsumer concurrently`).pipe(
                delay(100),
                tap(() => {
                    const myMessage = messageDictionary[messagePushedInTheDictionary];
                    expect(myMessage).to.be.not.undefined;
                    delete messageDictionary[messagePushedInTheDictionary];
                }),
            );
        };
        // The Producer sends a record
        sendRecord(producer, producerRecord)
            .pipe(
                // The consumer starts
                concatMap(() => doConsumer.consume()),
                take(messages.length), // to complete the Observable
                toArray(),
                tap(() => {
                    expect(messageDictionarySize.includes(concurrency)).to.be.true;

                    messageDictionarySize.forEach((size) => {
                        expect(size).to.be.lte(concurrency);
                    });
                }),
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    producer.disconnect();
                    doConsumer.disconnect();
                    done(err);
                },
                complete: () => {
                    producer.disconnect();
                    doConsumer.disconnect();
                    done();
                },
            });
    }).timeout(60000);
});

describe(`when concurrency is INCREASED after a DoConsumer has started consuming messages`, () => {
    let adminClient: Admin;
    const topicForDoConsumer = 'IncreaseConcurrency_DoConsumer_' + Date.now().toString();
    let producer: Producer;
    before(`create the Topic and the Producer`, (done) => {
        const clientId = 'IncreaseConcurrencyDoConsumer';
        const kafkaConfig: KafkaConfig = {
            clientId,
            brokers: testConfiguration.brokers,
            retry: {
                initialRetryTime: 100,
                retries: 3,
            },
        };
        const topics: ITopicConfig[] = [
            {
                topic: topicForDoConsumer,
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

    it(`more messages are processed concurrently`, (done) => {
        const numberOfMessages = 10;
        const messageValuesConcurrency_1 = new Array(numberOfMessages)
            .fill(null)
            .map((_, i) => `Message_${i}_concurrency_1_value for a DoConsumer`);
        const messagesConcurrency_1 = messageValuesConcurrency_1.map((value) => ({
            value,
        }));
        const producerRecordConcurrency_1: ProducerRecord = {
            messages: messagesConcurrency_1,
            topic: topicForDoConsumer,
        };
        const messageValuesConcurrency_N = new Array(numberOfMessages)
            .fill(null)
            .map((_, i) => `Message_${i}_concurrency_N_value for a DoConsumer`);
        const messagesConcurrency_N = messageValuesConcurrency_N.map((value) => ({
            value,
        }));
        const producerRecordConcurrency_N: ProducerRecord = {
            messages: messagesConcurrency_N,
            topic: topicForDoConsumer,
        };
        // Create the DoConsumer starting with a certain concurrency
        let currentConcurrency = 1;
        const doConsumer = new DoConsumer(
            'My Test Do Consumer Processing Messages with increased concurrency',
            0,
            testConfiguration.brokers,
            topicForDoConsumer,
            'DoConsumer_Test_ProcessMessagesWithIncreasedConcurrency',
            currentConcurrency,
        );
        // In order to check the level of concurrency,
        // the function pushes a message in a dictionary, registers the size of the dictionary
        // and wait for some time before removing the message and returning
        // If the processing is performed concurrently with the expected level of concurrency
        // the dictionary size at some point should reach the size of the expected concurrency
        // and should never exceed it
        let messageDictionary: { [message: string]: string } = {};
        let messageDictionarySize: number[] = [];
        doConsumer.doer = (message) => {
            const messagePushedInTheDictionary = message.value.toString();
            messageDictionary[messagePushedInTheDictionary] = messagePushedInTheDictionary;
            messageDictionarySize.push(Object.keys(messageDictionary).length);
            return of(`${messagePushedInTheDictionary} processed by DoConsumer`).pipe(
                delay(400),
                tap(() => {
                    const myMessage = messageDictionary[messagePushedInTheDictionary];
                    console.log(messagePushedInTheDictionary);
                    expect(myMessage).to.be.not.undefined;
                    delete messageDictionary[messagePushedInTheDictionary];
                }),
            );
        };

        const sendRecordsSubscriptions: Subscription[] = [];
        // The Producer sends the first set of records
        sendRecordsSubscriptions.push(sendRecord(producer, producerRecordConcurrency_1).subscribe());

        let waitTime = 5000;
        // After some time we increase the concurrency level
        setTimeout(() => {
            const concurrencyVariation = 4;
            currentConcurrency = currentConcurrency + concurrencyVariation;
            doConsumer.changeConcurrency(concurrencyVariation);
        }, waitTime);
        // After some more time the Producer sends the second set of records
        waitTime = waitTime + waitTime;
        setTimeout(() => {
            sendRecordsSubscriptions.push(sendRecord(producer, producerRecordConcurrency_N).subscribe());
        }, waitTime);

        // The consumer starts consuming messages
        doConsumer
            .consume()
            .pipe(
                take(messagesConcurrency_1.length + messagesConcurrency_N.length), // to complete the Observable
                tap((message) => {
                    if (!messageDictionarySize.includes(currentConcurrency)) {
                        console.log(messageDictionarySize);
                    }
                    expect(messageDictionarySize.includes(currentConcurrency)).to.be.true;

                    messageDictionarySize.forEach((size) => {
                        if (size > currentConcurrency) {
                            console.log(message);
                        }
                        expect(size).to.be.lte(currentConcurrency);
                    });
                }),
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    producer.disconnect();
                    doConsumer.disconnect();
                    done(err);
                },
                complete: () => {
                    expect(sendRecordsSubscriptions.length).to.equal(2);
                    sendRecordsSubscriptions.forEach((s) => s.unsubscribe());
                    producer.disconnect();
                    doConsumer.disconnect();
                    done();
                },
            });
    }).timeout(600000);
});

describe(`when concurrency is DECREASED after a DoConsumer has started consuming messages`, () => {
    let adminClient: Admin;
    const topicForDoConsumer = 'DecreasseConcurrency_DoConsumer_' + Date.now().toString();
    let producer: Producer;
    before(`create the Topic and the Producer`, (done) => {
        const clientId = 'DecreaseConcurrencyDoConsumer';
        const kafkaConfig: KafkaConfig = {
            clientId,
            brokers: testConfiguration.brokers,
            retry: {
                initialRetryTime: 100,
                retries: 3,
            },
        };
        const topics: ITopicConfig[] = [
            {
                topic: topicForDoConsumer,
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

    it(`less messages are processed concurrently`, (done) => {
        const numberOfMessages = 10;
        const messageValuesConcurrency_1 = new Array(numberOfMessages)
            .fill(null)
            .map((_, i) => `Message_${i}_concurrency_1_value for a DoConsumer`);
        const messageValuesConcurrency_N = new Array(numberOfMessages)
            .fill(null)
            .map((_, i) => `Message_${i}_concurrency_N_value for a DoConsumer`);
        const messagesConcurrency_N = messageValuesConcurrency_N.map((value) => ({
            value,
        }));
        const messagesConcurrency_1 = messageValuesConcurrency_1.map((value) => ({
            value,
        }));
        const producerRecordConcurrency_N: ProducerRecord = {
            messages: messagesConcurrency_N,
            topic: topicForDoConsumer,
        };
        const producerRecordConcurrency_1: ProducerRecord = {
            messages: messagesConcurrency_1,
            topic: topicForDoConsumer,
        };
        // Create the DoConsumer starting with a certain concurrency
        let currentConcurrency = 4;
        const consumerGroup = 'DoConsumer_Test_ProcessMessagesWithDecreasingConcurrency';
        const doConsumer = new DoConsumer(
            'My Test Do Consumer Processing Messages with decreasing concurrency',
            0,
            testConfiguration.brokers,
            topicForDoConsumer,
            consumerGroup,
            currentConcurrency,
        );
        // In order to check the level of concurrency,
        // the function pushes a message in a dictionary, registers the size of the dictionary
        // and wait for some time before removing the message and returning
        // If the processing is performed concurrently with the expected level of concurrency
        // the dictionary size at some point should reach the size of the expected concurrency
        // and should never exceed it
        let messageDictionary: { [message: string]: string } = {};
        let messageDictionarySize: number[] = [];
        doConsumer.doer = (message) => {
            const messagePushedInTheDictionary = message.value.toString();
            messageDictionary[messagePushedInTheDictionary] = messagePushedInTheDictionary;
            messageDictionarySize.push(Object.keys(messageDictionary).length);
            return of(`${messagePushedInTheDictionary} processed by DoConsumer`).pipe(
                delay(400),
                tap(() => {
                    const myMessage = messageDictionary[messagePushedInTheDictionary];
                    console.log(messagePushedInTheDictionary);
                    expect(myMessage).to.be.not.undefined;
                    delete messageDictionary[messagePushedInTheDictionary];
                }),
            );
        };

        const sendRecordsSubscriptions: Subscription[] = [];
        // The Producer sends the first set of records
        sendRecordsSubscriptions.push(sendRecord(producer, producerRecordConcurrency_N).subscribe());

        let waitTime = 5000;
        // After some time we decrease the concurrency level
        setTimeout(() => {
            const concurrencyVariation = -3;
            currentConcurrency = currentConcurrency + concurrencyVariation;
            doConsumer.changeConcurrency(concurrencyVariation);
        }, waitTime);
        // After some more time the Producer sends the second set of records
        waitTime = waitTime + waitTime;
        setTimeout(() => {
            // reset messageDictionary and messageDictionarySize to allow the check of max concurrency to work
            // considering that we are decreasing concurrency and therefore from now on the max expected concurrency
            // is the new decreased one
            messageDictionary = {};
            messageDictionarySize = [];
            sendRecordsSubscriptions.push(sendRecord(producer, producerRecordConcurrency_1).subscribe());
        }, waitTime);

        // The consumer starts consuming messages
        doConsumer
            .consume()
            .pipe(
                take(messagesConcurrency_N.length + messagesConcurrency_1.length), // to complete the Observable
                tap((message) => {
                    if (!messageDictionarySize.includes(currentConcurrency)) {
                        console.log(messageDictionarySize);
                    }
                    expect(messageDictionarySize.includes(currentConcurrency)).to.be.true;

                    messageDictionarySize.forEach((size) => {
                        if (size > currentConcurrency) {
                            console.log(message);
                        }
                        expect(size).to.be.lte(currentConcurrency);
                    });
                }),
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    producer.disconnect();
                    doConsumer.disconnect();
                    done(err);
                },
                complete: () => {
                    expect(sendRecordsSubscriptions.length).to.equal(2);
                    sendRecordsSubscriptions.forEach((s) => s.unsubscribe());
                    producer.disconnect();
                    doConsumer.disconnect();
                    done();
                },
            });
    }).timeout(600000);
});
