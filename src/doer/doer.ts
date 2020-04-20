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
    sendRecord,
    connectProducer,
} from '../observable-kafkajs/observable-kafkajs';
import { concatMap, tap, mergeMap, switchMap, map, takeUntil } from 'rxjs/operators';
import { Observable, BehaviorSubject, Subscription, of, Subject, forkJoin } from 'rxjs';
import { cpuUsageStream } from './cpu-usage';
import { newMessageRecord } from './messages';
import { parseCommand } from './commands';

export type doerFuntion = (message: KafkaMessage) => Observable<any>;

export const MESSAGES_FROM_ORCHESTRATOR_TOPIC_PREFIX = 'Orch_Msg_';
export function messagesFromOrchestratorTopicNamePrefix(doerName: string) {
    return `${MESSAGES_FROM_ORCHESTRATOR_TOPIC_PREFIX}${doerName}_`;
}
export function messagesToOrchestratorTopicName(doerName: string, doerId: number) {
    return `${messagesFromOrchestratorTopicNamePrefix(doerName)}${doerName}_${doerId}`;
}

export const COMMANDS_TO_ORCHESTRATOR_TOPIC_PREFIX = 'Orch_Cmd_';
export function commandsToOrchestratorTopicNamePrefix(doerName: string) {
    return `${COMMANDS_TO_ORCHESTRATOR_TOPIC_PREFIX}${doerName}_`;
}
export function commandsFromOrchestratorTopicName(doerName: string, doerId: number) {
    return `${commandsToOrchestratorTopicNamePrefix(doerName)}${doerName}_${doerId}`;
}

export class Doer {
    private messageConsumer: Consumer;
    private orchestratorCommandsConsumer: Consumer;
    private _changeConcurrency = new BehaviorSubject<number>(this._concurrency);
    private cpuUsageStreamSubscription: Subscription;
    public groupIdJoined = new Subject<string>();
    private removeGroupIdJoinedListener: RemoveInstrumentationEventListener<string>;
    private producer: Producer;
    private messagesToOrchestratorTopic: string;
    private commandsFromOrchestratorTopic: string;
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
        this.messagesToOrchestratorTopic = messagesToOrchestratorTopicName(this.name, this.id);
        this.commandsFromOrchestratorTopic = commandsFromOrchestratorTopicName(this.name, this.id);
    }

    do: doerFuntion = (message: KafkaMessage) => {
        throw new Error(`No function specified for Doer ${this.name} - Message received ${message}`);
    };

    start() {
        console.log(`Elastic Doer ${this.name} (id: ${this.id}) starts`);

        forkJoin(this.processMessages(), this.processOrchestratorCommands())
            .pipe(
                takeUntil(
                    this.disconnected.pipe(
                        concatMap(() =>
                            sendRecord(this.producer, newMessageRecord('Stop', this.messagesToOrchestratorTopic)),
                        ),
                        tap(() => this._disconnect()),
                    ),
                ),
            )
            .subscribe({
                error: (err) => {
                    console.error('!!!!!!!!! Processing Error', err);
                    this._disconnect();
                },
                complete: () => {
                    console.log(`Elastic Doer ${this.name} (id: ${this.id}) completed`);
                },
            });
    }
    private processMessages() {
        // determines whether the processing is for either a pure Consumer or a ConsumerProducer
        const processChain: (concurrencyAdjustment?: boolean) => Observable<any> = (this.outputTopics.length > 0
            ? this.consumeProduce
            : this.consume
        ).bind(this);

        // connects the Producer of this Doer and starts the processing
        return connectProducer(this.kafkaConfig()).pipe(
            tap((producer) => (this.producer = producer)),
            concatMap(() => sendRecord(this.producer, newMessageRecord('Start', this.messagesToOrchestratorTopic))),
            concatMap(() => processChain()),
        );
    }
    private processOrchestratorCommands() {
        // connects the Consumer of this Doer to enable receiving commands from Orchestrator
        return connectConsumer(this.kafkaConfig(), `${this.name}_${this.id}_Orch_Commands`).pipe(
            tap((consumer) => (this.orchestratorCommandsConsumer = consumer)),
            concatMap(() =>
                subscribeConsumerToTopic(this.orchestratorCommandsConsumer, this.commandsFromOrchestratorTopic),
            ),
            concatMap(() => consumerMessages(this.orchestratorCommandsConsumer)),
            map((commandMessage) => parseCommand(commandMessage.kafkaMessage.value.toString())),
            tap((command) => {
                switch (command.commandId) {
                    case 'END':
                        this.disconnect();
                        break;
                    default:
                        console.error(`Command id ${command.commandId} not supported`);
                }
            }),
        );
    }

    consume(concurrencyAdjustment = false) {
        if (concurrencyAdjustment) {
            this.startAutomaticConcurrencyAdjustment();
        }
        return connectConsumer(this.kafkaConfig(), this.consumerGroup).pipe(
            tap((consumer) => (this.messageConsumer = consumer)),
            tap(() => {
                this.removeGroupIdJoinedListener = this.messageConsumer.on(
                    this.messageConsumer.events.GROUP_JOIN,
                    (e) => {
                        console.log(`Consumer joined Group Id at ${JSON.stringify(e)}`);
                        this.groupIdJoined.next();
                    },
                );
            }),
            concatMap(() => subscribeConsumerToTopic(this.messageConsumer, this.inputTopic)),
            concatMap(() => this._changeConcurrency),
            tap(() => console.log('!!!!!!!!! concurrency is ', this._concurrency)),
            switchMap(() =>
                consumerMessages(this.messageConsumer).pipe(
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
    private _disconnect() {
        this.cpuUsageStreamSubscription?.unsubscribe();
        if (this.removeGroupIdJoinedListener) {
            this.removeGroupIdJoinedListener();
        }
        this.producer?.disconnect();
        this.orchestratorCommandsConsumer?.disconnect();
        this.messageConsumer?.disconnect();
        if (
            !this.removeGroupIdJoinedListener ||
            !this.producer ||
            !this.orchestratorCommandsConsumer ||
            !this.messageConsumer
        ) {
            process.exit(100);
        }
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
