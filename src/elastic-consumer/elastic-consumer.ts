import { KafkaMessage, KafkaConfig, Consumer } from 'kafkajs';

import { connectConsumer, subscribeConsumerToTopic, consumerMessages } from '../observable-kafkajs/observable-kafkajs';
import { concatMap, tap, mergeMap, switchMap } from 'rxjs/operators';
import { Observable, BehaviorSubject, Subscription } from 'rxjs';
import { cpuUsageStream } from './cpu-usage';

export abstract class ElasticConsumer<T> {
    protected consumer: Consumer;
    private _changeConcurrency = new BehaviorSubject<number>(this._concurrency);
    private cpuUsageStreamSubscription: Subscription;

    constructor(
        protected name: string,
        protected id: number,
        protected brokers: string[],
        protected topic: string,
        protected consumerGroup: string,
        private _concurrency = 1,
        private _cpuUsageCheckFrequency = 2000,
        private _cpuUsageThreshold = 60,
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

    consume(concurrencyAdjustment = false) {
        if (concurrencyAdjustment) {
            this.startAutomaticConcurrencyAdjustment();
        }
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
        this.cpuUsageStreamSubscription?.unsubscribe();
        return this.consumer.disconnect();
    }

    get concurrency() {
        return this._concurrency;
    }
    changeConcurrency(variation: number) {
        this._concurrency = this._concurrency + variation <= 1 ? 1 : this._concurrency + variation;
        this._changeConcurrency.next(this._concurrency);
    }
    startAutomaticConcurrencyAdjustment() {
        this.cpuUsageStreamSubscription = cpuUsageStream(this._cpuUsageCheckFrequency)
            .pipe(
                tap((_cpuUsage) => {
                    if (_cpuUsage > this._cpuUsageThreshold) {
                        this.changeConcurrency(-1);
                    } else if (_cpuUsage < this._cpuUsageThreshold * 0.6) {
                        this.changeConcurrency(1);
                    }
                }),
            )
            .subscribe();
    }
}
