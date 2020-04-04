import { Producer, Message, ProducerRecord } from 'kafkajs';

import { connectProducer, sendRecord } from '../observable-kafkajs/observable-kafkajs';
import { concatMap, tap, map, mergeMap } from 'rxjs/operators';
import { ElasticConsumer } from '../elastic-consumer/elastic-consumer';
import { forkJoin } from 'rxjs';

export abstract class ElasticConsumerProducer<T> extends ElasticConsumer<T> {
    private producer: Producer;

    constructor(
        name: string,
        id: number,
        brokers: string[],
        topic: string,
        consumerGroup: string,
        private producerTopics: string[],
    ) {
        super(name, id, brokers, topic, consumerGroup);
    }

    abstract messageForProducer(result: T): string | Buffer;

    start() {
        this.consumeProduce().subscribe({
            error: (err) => {
                console.error(err);
            },
            complete: () => {
                console.log(`Elastic Consumer ${this.name} (id: ${this.id}) completed`);
            },
        });
    }

    consumeProduce() {
        const kafkaConfig = this.kafkaConfig();
        return connectProducer(kafkaConfig).pipe(
            tap((producer) => (this.producer = producer)),
            concatMap(() => super.consume()),
            map((result) => {
                const messages: Message[] = [
                    {
                        value: this.messageForProducer(result),
                    },
                ];
                return messages;
            }),
            mergeMap((messages) => {
                const sendRecords = this.producerTopics.map((topic) => {
                    const producerRecord: ProducerRecord = {
                        messages,
                        topic,
                    };
                    return sendRecord(this.producer, producerRecord);
                });
                return forkJoin(sendRecords);
            }),
        );
    }

    disconnect() {
        return super.disconnect().then(() => this.producer.disconnect());
    }
}
