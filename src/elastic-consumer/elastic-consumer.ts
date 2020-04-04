import { KafkaMessage, KafkaConfig, Consumer } from 'kafkajs';

import { connectConsumer, subscribeConsumerToTopic, consumerMessages } from '../observable-kafkajs/observable-kafkajs';
import { concatMap, tap, map } from 'rxjs/operators';

export abstract class ElasticConsumer<T> {
    protected consumer: Consumer;

    constructor(
        protected name: string,
        protected id: number,
        protected brokers: string[],
        protected topic: string,
        protected consumerGroup: string,
    ) {}

    abstract processMessage(message: KafkaMessage): T;

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
            map((message) => this.processMessage(message.kafkaMessage)),
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
