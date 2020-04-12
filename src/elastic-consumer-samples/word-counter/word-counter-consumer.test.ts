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
import { WordCounterConsumer } from './word-counter-consumer';
import { join } from 'path';
import { cpuUsage } from 'process';

describe(`when a WordCounterConsumer reads the file name from the topic`, () => {
    let adminClient: Admin;
    const topicForWordCounterConsumer = '_WordCounterConsumer_' + Date.now().toString();
    let producer: Producer;
    before(`create the Topic and the Producer`, (done) => {
        const clientId = 'WordCounterConsumer';
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
                topic: topicForWordCounterConsumer,
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

    it(`it reads the file and count the occurrences of each word in the file`, (done) => {
        const tests = {
            billy: {
                file: '../../../big-text-files/The_Complete_Works_of_William_Shakespeare.txt',
                occurrencesOfLove: 2268,
            },
            wiki_100MB: { file: '../../../../../very-large-files/enwik8', occurrencesOfLove: 1672 },
            wiki_1GB: { file: '../../../../../very-large-files/enwik9', occurrencesOfLove: 18724 },
        };
        const test = tests.wiki_100MB;
        const file = join(__dirname, test.file);
        const messages: Message[] = [
            {
                value: file,
            },
        ];
        const producerRecord: ProducerRecord = {
            messages,
            topic: topicForWordCounterConsumer,
        };
        const consumerGroup = 'WordCounterConsumer_Test_CountWOrdsOfOneFile';
        // Create the consumer
        const wordCounterConsumer = new WordCounterConsumer(
            'My Test WordCounterConsumer',
            0,
            testConfiguration.brokers,
            topicForWordCounterConsumer,
            consumerGroup,
        );
        // The Producer sends a record
        let startDate = Date.now();
        let previousUsage = cpuUsage();
        sendRecord(producer, producerRecord)
            .pipe(
                // The consumer starts
                concatMap(() => wordCounterConsumer.consume()),
                tap((wordCounts) => {
                    expect(Object.keys(wordCounts).length).to.be.gt(0);
                    expect(wordCounts['love']).to.equal(test.occurrencesOfLove);
                    const wordsCounted = new Intl.NumberFormat().format(Object.keys(wordCounts).length);
                    console.log(`>>>>>>>>>>>>${wordsCounted} words counted`);
                    const usage = cpuUsage(previousUsage);
                    const result = (100 * (usage.user + usage.system)) / ((Date.now() - startDate) * 1000);
                    console.log(`>>>>>>>>>>>>CPU used ${result}%`);
                }),
                take(1), // to complete the Observable
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    producer.disconnect();
                    wordCounterConsumer.disconnect();
                    done(err);
                },
                complete: () => {
                    producer.disconnect();
                    wordCounterConsumer.disconnect();
                    done();
                },
            });
    }).timeout(600000);
});

describe.only(`when a WordCounterConsumer reads some file names from the topic`, () => {
    let adminClient: Admin;
    const topicForWordCounterConsumer = 'ManyFiles_WordCounterConsumer_' + Date.now().toString();
    let producer: Producer;
    before(`create the Topic and the Producer`, (done) => {
        const clientId = 'ManyFilesWordCounterConsumer';
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
                topic: topicForWordCounterConsumer,
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

    it(`it reads the files and count the occurrences of each word in each file`, (done) => {
        const tests = {
            billy: {
                file: '../../../big-text-files/The_Complete_Works_of_William_Shakespeare.txt',
                occurrencesOfLove: 2268,
            },
            wiki_100MB: { file: '../../../../../very-large-files/enwik8', occurrencesOfLove: 1672 },
            wiki_1GB: { file: '../../../../../very-large-files/enwik9', occurrencesOfLove: 18724 },
            wiki_1GB_copy: { file: '../../../../../very-large-files/enwik9 copy', occurrencesOfLove: 18724 },
            wiki_1GB_copy_2: { file: '../../../../../very-large-files/enwik9 copy 2', occurrencesOfLove: 18724 },
            wiki_1GB_copy_3: { file: '../../../../../very-large-files/enwik9 copy 3', occurrencesOfLove: 18724 },
        };
        const test = [tests.wiki_1GB, tests.wiki_1GB_copy, tests.wiki_1GB_copy_2, tests.wiki_1GB_copy_3];
        const messages: Message[] = test.map((t) => ({
            value: join(__dirname, t.file),
        }));
        const producerRecord: ProducerRecord = {
            messages,
            topic: topicForWordCounterConsumer,
        };
        const consumerGroup = 'WordCounterConsumer_Test_CountWOrdsOfOneFile';
        // Create the consumer
        const wordCounterConsumer = new WordCounterConsumer(
            'My Test WordCounterConsumer for many files',
            0,
            testConfiguration.brokers,
            topicForWordCounterConsumer,
            consumerGroup,
            test.length,
        );
        // The Producer sends a record
        let startDate = Date.now();
        let previousUsage = cpuUsage();
        sendRecord(producer, producerRecord)
            .pipe(
                // The consumer starts
                concatMap(() => wordCounterConsumer.consume()),
                tap((wordCounts) => {
                    expect(Object.keys(wordCounts).length).to.be.gt(0);
                    // expect(wordCounts['love']).to.equal(test.occurrencesOfLove);
                    const wordsCounted = new Intl.NumberFormat().format(Object.keys(wordCounts).length);
                    console.log(`>>>>>>>>>>>>${wordsCounted} words counted`);
                    const usage = cpuUsage(previousUsage);
                    const result = (100 * (usage.user + usage.system)) / ((Date.now() - startDate) * 1000);
                    console.log(`>>>>>>>>>>>>CPU used ${result}%`);
                }),
                take(1), // to complete the Observable
            )
            .subscribe({
                error: (err) => {
                    console.error('ERROR', err);
                    producer.disconnect();
                    wordCounterConsumer.disconnect();
                    done(err);
                },
                complete: () => {
                    producer.disconnect();
                    wordCounterConsumer.disconnect();
                    done();
                },
            });
    }).timeout(600000);
});
