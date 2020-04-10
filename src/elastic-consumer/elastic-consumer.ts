import { KafkaMessage, KafkaConfig, Consumer } from 'kafkajs';

import { connectConsumer, subscribeConsumerToTopic, consumerMessages } from '../observable-kafkajs/observable-kafkajs';
import { concatMap, tap, map, mergeMap, switchMap } from 'rxjs/operators';
import { Observable, Subject, BehaviorSubject, of, from, defer } from 'rxjs';

export abstract class ElasticConsumer<T> {
    protected consumer: Consumer;
    increaseConcurrency: BehaviorSubject<number>;
    _increaseConcurrency: Observable<number>;

    constructor(
        protected name: string,
        protected id: number,
        protected brokers: string[],
        protected topic: string,
        protected consumerGroup: string,
        private concurrency = 1,
    ) {
        this.increaseConcurrency = new BehaviorSubject<number>(this.concurrency);
        this._increaseConcurrency = this.increaseConcurrency.pipe(
            map((_concurrency) => (this.concurrency = _concurrency)),
        );
    }

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
        const stream_ = () => {
            const start = this.consumer ? from(this.consumer.disconnect()) : of(null);
            return start.pipe(
                concatMap(() => connectConsumer(this.kafkaConfig(), this.consumerGroup)),
                tap((consumer) => (this.consumer = consumer)),
                concatMap(() => subscribeConsumerToTopic(this.consumer, this.topic)),
                tap(() => console.log('!!!!!!!!! concurrency is ', this.concurrency)),
                switchMap(() =>
                    consumerMessages(this.consumer).pipe(
                        mergeMap((message) => this.processMessage(message.kafkaMessage), this.concurrency),
                    ),
                ),
            );
        };
        return this._increaseConcurrency.pipe(switchMap(() => stream_()));
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
