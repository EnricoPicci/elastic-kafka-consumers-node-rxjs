import { KafkaMessage, KafkaConfig, Consumer } from 'kafkajs';

import { connectConsumer, subscribeConsumerToTopic, consumerMessages } from '../observable-kafkajs/observable-kafkajs';
import { concatMap, tap, map, mergeMap } from 'rxjs/operators';
import { Observable } from 'rxjs';

export abstract class ElasticConsumer<T> {
    protected consumer: Consumer;

    constructor(
        protected name: string,
        protected id: number,
        protected brokers: string[],
        protected topic: string,
        protected consumerGroup: string,
        private concurrency = 1,
    ) {}

    abstract processMessage(message: KafkaMessage): Observable<T>;

    start() {
        this.consume().subscribe({
            error: (err) => {
                console.error(err);
            },
            complete: () => {
                console.log(`Elastic Consumer ${this.name} (id: ${this.id}) completed`);
            },
        });
    }

    consume() {
        return connectConsumer(this.kafkaConfig(), this.consumerGroup).pipe(
            tap((consumer) => (this.consumer = consumer)),
            concatMap(() => subscribeConsumerToTopic(this.consumer, this.topic)),
            concatMap(() => consumerMessages(this.consumer)),
            mergeMap((message) => this.processMessage(message.kafkaMessage), this.concurrency),
        );
    }

    kafkaConfig() {
        const kafkaConfig: KafkaConfig = {
            clientId: this.name,
            brokers: this.brokers,
        };
        return kafkaConfig;
    }

    disconnect() {
        return this.consumer.disconnect();
    }
}
