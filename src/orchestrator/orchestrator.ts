import { join } from 'path';
import { readConfigFromFile, Config, DoerConfig } from './configuration/config';
import { tap, concatMap, map, take, mergeMap } from 'rxjs/operators';
import { from, of, Subject, forkJoin } from 'rxjs';
import { KafkaConfig, Admin, Consumer, Producer, ProducerRecord, Message } from 'kafkajs';
import {
    connectAdminClient,
    deleteTopics,
    fetchTopicMetadata,
    sendRecord,
    connectProducer,
    connectConsumer,
    consumerMessages,
    subscribeConsumerToTopic,
    ConsumerMessage,
} from '../observable-kafkajs/observable-kafkajs';
import {
    MESSAGES_TO_ORCHESTRATOR_TOPIC_PREFIX,
    commandsFromOrchestratorTopicName,
    COMMANDS_FROM_ORCHESTRATOR_TOPIC_PREFIX,
    messagesToOrchestratorTopicName,
} from '../doer/doer';
import { spawn } from 'child_process';
import { Command } from '../doer/commands';

type DoerInfo = {
    started: Date;
    ended?: Date;
    id: number;
    status: 'active' | 'ended' | 'failed' | 'starting';
    exitCode?: number;
    error?: Error;
};
export class Orchestrator {
    broker: string;
    kafkaConfig: KafkaConfig;
    adminClient: Admin;
    commandProducer: Producer;
    doerMessageConsumer: Consumer;

    config: Config;
    doers: { [doerId: string]: DoerInfo[] } = {};
    private _doerInfo$ = new Subject<DoerInfo>();
    doerInfo$ = this._doerInfo$.asObservable();
    private _doerMessage$ = new Subject<ConsumerMessage>();
    doerMessage$ = this._doerMessage$.asObservable();

    start(configFile: string, broker: string, doerToLaunch?: string) {
        this.broker = broker;
        this.kafkaConfig = {
            clientId: 'Orchestrator',
            brokers: [this.broker],
            retry: {
                initialRetryTime: 100,
                retries: 3,
            },
        };
        const filePath = join(__dirname, configFile);

        connectProducer(this.kafkaConfig)
            .pipe(
                tap((producer) => (this.commandProducer = producer)),
                concatMap(() => connectConsumer(this.kafkaConfig, 'Orchestrator')),
                tap((consumer) => (this.doerMessageConsumer = consumer)),
                // launch the Doers found in the configuration
                concatMap(() => readConfigFromFile(filePath)),
                tap((_config) => (this.config = _config)),
                concatMap(() => this.deleteOrchestratorMessageTopics()),
                concatMap(() => {
                    const _doers = doerToLaunch
                        ? [this.config.doers.find((d) => d.name === doerToLaunch)]
                        : this.config.doers;
                    if (_doers.length === 0) {
                        throw new Error(`No Doer with name ${doerToLaunch} is configured in ${filePath}`);
                    }
                    return forkJoin(_doers.map((d) => this.launchDoer(d)));
                }),
                concatMap(() => this.processMessagesFromDoers()),
            )
            .subscribe();
    }
    private deleteOrchestratorMessageTopics() {
        return connectAdminClient(this.kafkaConfig).pipe(
            tap((client) => (this.adminClient = client)),
            concatMap(() => fetchTopicMetadata(this.adminClient)),
            map((topicsMetadata) => {
                const orchestratorMessageTopicPrefixLength = MESSAGES_TO_ORCHESTRATOR_TOPIC_PREFIX.length;
                const orchestratorCommandTopicPrefixLength = COMMANDS_FROM_ORCHESTRATOR_TOPIC_PREFIX.length;
                return topicsMetadata.topics.filter(
                    (t) =>
                        t.name.substr(0, orchestratorMessageTopicPrefixLength) ===
                            MESSAGES_TO_ORCHESTRATOR_TOPIC_PREFIX ||
                        t.name.substr(0, orchestratorCommandTopicPrefixLength) ===
                            COMMANDS_FROM_ORCHESTRATOR_TOPIC_PREFIX,
                );
            }),
            concatMap((_topics) => {
                const topics = _topics.map((t) => t.name);
                return deleteTopics(this.adminClient, topics);
            }),
        );
    }

    private launchDoer(doer: DoerConfig) {
        if (!this.doers[doer.name]) {
            this.doers[doer.name] = [];
        }
        const doerSiblings = this.doers[doer.name];
        const doerId = this.doers[doer.name].length;
        const newDoerInfo: DoerInfo = { started: new Date(), id: doerSiblings.length, status: 'starting' };
        this.doers[doer.name].push(newDoerInfo);

        this.launchDoerAsChildProcess(doer, doerId);
        return subscribeConsumerToTopic(this.doerMessageConsumer, messagesToOrchestratorTopicName(doer.name, doerId));
    }
    private launchDoerAsChildProcess(doer: DoerConfig, doerId: number) {
        if (process.platform === 'win32') {
            throw new Error('Start command from windows to be implemented');
        }
        const command = `node`;
        const concurrency = doer.concurrency ? doer.concurrency : '1';
        const args = [
            `./dist/doer/launch-doer`,
            doer.name,
            doerId + '',
            this.broker,
            doer.inputTopic,
            concurrency,
            JSON.stringify(doer.outputTopics),
            doer.function,
            doer.functionModule,
        ];
        const doerProcess = spawn(command, args);
        const doerInfo = this.doers[doer.name][doerId];
        doerProcess.on('error', (err) => {
            doerInfo.ended = new Date();
            doerInfo.status = 'failed';
            doerInfo.error = err;
            this._doerInfo$.next(doerInfo);
            console.error(`Failed to start Doer ${doer.name} (id: ${doerId})`, err);
        });

        doerProcess.on('close', (code) => {
            doerInfo.ended = new Date();
            doerInfo.status = 'ended';
            doerInfo.exitCode;
            this._doerInfo$.next(doerInfo);
            console.log(`Doer ${doer.name} (id: ${doerId}) process exited with code ${code}`);
        });
        doerProcess.stdout.on('data', (data) => {
            console.log(`Doer ${doer.name} (id: ${doerId}) stdout: ${data}`);
        });
        doerProcess.stderr.on('data', (data) => {
            console.log(`Doer ${doer.name} (id: ${doerId}) stderr: ${data}`);
        });

        this._doerInfo$.next(doerInfo);
        console.log(`Doer ${doer.name} (id: ${doerId}) launched`);
    }

    private processMessagesFromDoers() {
        return consumerMessages(this.doerMessageConsumer).pipe(
            tap((message) => {
                console.log('^^^^^^^^^^^^^^^^^^^^^^', message.topic, message.kafkaMessage.value.toString());
                this._doerMessage$.next(message);
            }),
        );
    }

    disconnect() {
        this.adminClient.disconnect();
        this.commandProducer.disconnect();
        this.doerMessageConsumer.disconnect();
    }

    sendCommand(command: Command, doerName: string, doerId: number) {
        const commandTopic = commandsFromOrchestratorTopicName(doerName, doerId);
        const message: Message = { value: JSON.stringify(command) };
        const commandRecord: ProducerRecord = {
            messages: [message],
            topic: commandTopic,
        };
        sendRecord(this.commandProducer, commandRecord)
            .pipe(
                take(1), // make sure we complete the stream after sending the command
            )
            .subscribe({
                error: (err) => console.error(err),
            });
    }
}
