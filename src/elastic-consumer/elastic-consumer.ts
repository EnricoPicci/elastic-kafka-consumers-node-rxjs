import { KafkaMessage, KafkaConfig, Consumer } from 'kafkajs';

import { connectConsumer, subscribeConsumerToTopic, consumerMessages } from '../observable-kafkajs/observable-kafkajs';
import { concatMap, tap, map } from 'rxjs/operators';

export abstract class ElasticConsumer<T> {
    private consumer: Consumer;

    constructor(
        private name: string,
        private brokers: string[],
        private topic: string,
        private consumerGroup: string,
    ) {}

    abstract processMessage(message: KafkaMessage): T;

    start() {
        const kafkaConfig: KafkaConfig = {
            clientId: this.name,
            brokers: this.brokers,
        };
        return connectConsumer(kafkaConfig, this.consumerGroup).pipe(
            tap(consumer => (this.consumer = consumer)),
            concatMap(() => subscribeConsumerToTopic(this.consumer, this.topic)),
            concatMap(() => consumerMessages(this.consumer)),
            map(message => this.processMessage(message.kafkaMessage)),
        );
    }

    disconnect() {
        return this.consumer.disconnect();
    }
}
