import { KafkaMessage, KafkaConfig, Consumer } from 'kafkajs';

import { connectConsumer, subscribeConsumerToTopic, consumerMessages } from '../observable-kafkajs/observable-kafkajs';
import { concatMap, tap, map, mergeMap, switchMap, scan } from 'rxjs/operators';
import { Observable, BehaviorSubject, Subject } from 'rxjs';

export abstract class ElasticConsumer<T> {
    protected consumer: Consumer;
    private _changeConcurrency = new BehaviorSubject<number>(this._concurrency);
    increaseConcurrency: BehaviorSubject<number>;
    _increaseConcurrency: Observable<number>;

    constructor(
        protected name: string,
        protected id: number,
        protected brokers: string[],
        protected topic: string,
        protected consumerGroup: string,
        private _concurrency = 1,
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
            concatMap(() => this._changeConcurrency),
            tap(() => console.log('!!!!!!!!! concurrency is ', this._concurrency)),
            switchMap(() =>
                consumerMessages(this.consumer).pipe(
                    mergeMap((message) => this.processMessage(message.kafkaMessage), this._concurrency),
                ),
            ),
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

    get concurrency() {
        return this._concurrency;
    }
    changeConcurrency(variation: number) {
        this._concurrency = this._concurrency + variation <= 1 ? 1 : this._concurrency + variation;
        this._changeConcurrency.next(this._concurrency);
    }
}
