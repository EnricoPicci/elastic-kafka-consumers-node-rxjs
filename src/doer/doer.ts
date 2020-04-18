import {
    KafkaMessage,
    KafkaConfig,
    Consumer,
    RemoveInstrumentationEventListener,
    Producer,
    Message,
    ProducerRecord,
} from 'kafkajs';

import {
    connectConsumer,
    subscribeConsumerToTopic,
    consumerMessages,
    connectProducer,
    sendRecord,
} from '../observable-kafkajs/observable-kafkajs';
import { concatMap, tap, mergeMap, switchMap, map, takeUntil, finalize } from 'rxjs/operators';
import { Observable, BehaviorSubject, Subscription, of, Subject, forkJoin } from 'rxjs';
import { cpuUsageStream } from './cpu-usage';
import { newMessageRecord } from './messages';

export type doerFuntion = (message: KafkaMessage) => Observable<any>;

export class Doer {
    protected consumer: Consumer;
    private _changeConcurrency = new BehaviorSubject<number>(this._concurrency);
    private cpuUsageStreamSubscription: Subscription;
    public groupIdJoined = new Subject<string>();
    private removeGroupIdJoinedListener: RemoveInstrumentationEventListener<string>;
    private producer: Producer;
    private msgToOrchTopic: string;
    private disconnected = new Subject<void>();

    constructor(
        protected name: string,
        protected id: number,
        protected brokers: string[],
        protected inputTopic: string,
        protected consumerGroup: string,
        protected outputTopics: string[] = [],
        private _concurrency = 1,
        private _cpuUsageCheckFrequency = 2000,
        private _cpuUsageThreshold = 60,
    ) {
        this.msgToOrchTopic = `${this.name}_${this.id}_2_Orch`;
    }

    do: doerFuntion = (message: KafkaMessage) => {
        throw new Error(`No function specified for Doer ${this.name} - Message received ${message}`);
    };

    start() {
        console.log(`Elastic Doer ${this.name} (id: ${this.id}) starts`);
        const processChain: (concurrencyAdjustment?: boolean) => Observable<any> = (this.outputTopics.length > 0
            ? this.consumeProduce
            : this.consume
        ).bind(this);

        const kafkaConfig = this.kafkaConfig();
        connectProducer(kafkaConfig)
            .pipe(
                tap((producer) => (this.producer = producer)),
                concatMap(() => sendRecord(this.producer, newMessageRecord('Started', this.msgToOrchTopic))),
                concatMap(() => processChain()),
                takeUntil(
                    this.disconnected.pipe(
                        concatMap(() => sendRecord(this.producer, newMessageRecord('Stopped', this.msgToOrchTopic))),
                        tap(() => this._disconnect()),
                    ),
                ),
            )
            .subscribe({
                error: (err) => {
                    console.error(err);
                },
                complete: () => {
                    console.log(`Elastic Doer ${this.name} (id: ${this.id}) completed`);
                },
            });
    }

    consume(concurrencyAdjustment = false) {
        if (concurrencyAdjustment) {
            this.startAutomaticConcurrencyAdjustment();
        }
        return connectConsumer(this.kafkaConfig(), this.consumerGroup).pipe(
            tap((consumer) => (this.consumer = consumer)),
            tap(() => {
                this.removeGroupIdJoinedListener = this.consumer.on(this.consumer.events.GROUP_JOIN, (e) => {
                    console.log(`Consumer joined Group Id at ${JSON.stringify(e)}`);
                    this.groupIdJoined.next();
                });
            }),
            concatMap(() => subscribeConsumerToTopic(this.consumer, this.inputTopic)),
            concatMap(() => this._changeConcurrency),
            tap(() => console.log('!!!!!!!!! concurrency is ', this._concurrency)),
            switchMap(() =>
                consumerMessages(this.consumer).pipe(
                    mergeMap((message) => this.do(message.kafkaMessage), this._concurrency),
                ),
            ),
            // share(), // added to allow tests to subscribe to this Observable
        );
    }
    consumeProduce(concurrencyAdjustment = false) {
        return this.consume(concurrencyAdjustment).pipe(
            map((result) => {
                const messages: Message[] = [
                    {
                        value: result,
                    },
                ];
                return messages;
            }),
            mergeMap((messages) => {
                const sendRecords = this.outputTopics.map((topic) => {
                    const producerRecord: ProducerRecord = {
                        messages,
                        topic,
                    };
                    return sendRecord(this.producer, producerRecord);
                });
                return forkJoin(sendRecords);
            }),
            // share(), // added to allow tests to subscribe to this Observable
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
        this.disconnected.next();
    }
    _disconnect() {
        this.disconnected.next();
        this.cpuUsageStreamSubscription?.unsubscribe();
        this.removeGroupIdJoinedListener();
        this.producer.disconnect();
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
