import 'mocha';
import { expect } from 'chai';
import { Orchestrator } from './orchestrator';
import { ProducerRecord, KafkaConfig, Producer, Admin } from 'kafkajs';
import { connectProducer, deleteTopics, connectAdminClient } from '../observable-kafkajs/observable-kafkajs';
import { testConfiguration } from '../observable-kafkajs/test-config';
import { tap, concatMap, find, delay } from 'rxjs/operators';
import { Command } from '../doer/commands';
import { commandsFromOrchestratorTopicName } from '../doer/doer';

function kafkaConfig(clientId: string): KafkaConfig {
    return {
        clientId,
        brokers: testConfiguration.brokers,
        retry: {
            initialRetryTime: 100,
            retries: 3,
        },
    };
}

describe(`when an Orchestrator launches one Doer as a process child`, () => {
    // it(`it launches all doers specified in its config file and receives the Started message from them`, (done) => {
    //     const configFileName = 'orchestrator-test.config.json';
    //     const orchestrator = new Orchestrator();
    //     orchestrator.start(configFileName, 'localhost:9092');
    //     done();
    // });
    it(`if the function of the Doer fails, the Doer process ends and the Orchestrator receives a notification`, (done) => {
        const configFileName = 'orchestrator-test.config.json';
        const orchestrator = new Orchestrator();
        orchestrator.start(configFileName, 'localhost:9092', 'fail_doer');

        const doerInputTopic = 'input_topic_fail_doer';
        const producerMessage = 'a message';
        const producerRecord: ProducerRecord = {
            messages: [
                {
                    value: producerMessage,
                },
            ],
            topic: doerInputTopic,
        };
        const clientId = 'Orchestrator test';
        let adminClient: Admin;
        let producer: Producer;
        connectAdminClient(kafkaConfig(clientId))
            .pipe(
                tap((_adminClient) => (adminClient = _adminClient)),
                concatMap(() => deleteTopics(adminClient, [doerInputTopic])),
                concatMap(() => connectProducer(kafkaConfig(clientId))),
                tap((_producer) => (producer = _producer)),
                concatMap(() => producer.send(producerRecord)),
                concatMap(() => orchestrator.doerInfo$),
                find((doerInfo) => doerInfo.status === 'ended'),
            )
            .subscribe({
                error: (e) => {
                    producer.disconnect();
                    orchestrator.disconnect();
                    adminClient.disconnect();
                    done(e);
                },
                complete: () => {
                    producer.disconnect();
                    orchestrator.disconnect();
                    adminClient.disconnect();
                    done();
                },
            });
    }).timeout(200000);
});

describe(`when an Orchestrator launches one Doer`, () => {
    describe(`and send it a command to END`, () => {
        it(`the Doer ends and the Orchestrator receives a notification`, (done) => {
            const configFileName = 'orchestrator-test.config.json';
            const orchestrator = new Orchestrator();
            const doerName = 'doer_1';
            orchestrator.start(configFileName, 'localhost:9092', doerName);

            const command: Command = {
                commandId: 'END',
            };
            const commandTopic = commandsFromOrchestratorTopicName(doerName, 0);
            const endCommandRecord: ProducerRecord = {
                messages: [
                    {
                        value: JSON.stringify(command),
                    },
                ],
                topic: commandTopic,
            };
            const clientId = 'Orchestrator_test_END_command';
            let adminClient: Admin;
            let producer: Producer;
            connectAdminClient(kafkaConfig(clientId))
                .pipe(
                    tap((_adminClient) => (adminClient = _adminClient)),
                    concatMap(() => deleteTopics(adminClient, [commandTopic])),
                    concatMap(() => connectProducer(kafkaConfig(clientId))),
                    tap((_producer) => (producer = _producer)),
                    delay(10000), // delay to increase the possibility that the Doer is up when we send the END command
                    concatMap(() => producer.send(endCommandRecord)),
                    concatMap(() => orchestrator.doerInfo$),
                    tap((d) => {
                        console.log('&&&&&&&&&&&&&&&&&&&&&&&&&& message received ', d);
                    }),
                    find((doerInfo) => doerInfo.status === 'ended'),
                )
                .subscribe({
                    error: (e) => {
                        producer.disconnect();
                        adminClient.disconnect();
                        orchestrator.disconnect();
                        done(e);
                    },
                    complete: () => {
                        producer.disconnect();
                        adminClient.disconnect();
                        orchestrator.disconnect();
                        done();
                    },
                });
        }).timeout(100000);
        it.only(`the Orchestrator receives a stop message`, (done) => {
            const configFileName = 'orchestrator-test.config.json';
            const orchestrator = new Orchestrator();
            const doerName = 'doer_2';
            orchestrator.start(configFileName, 'localhost:9092', doerName);

            const command: Command = {
                commandId: 'END',
            };
            const commandTopic = commandsFromOrchestratorTopicName(doerName, 0);
            const endCommandRecord: ProducerRecord = {
                messages: [
                    {
                        value: JSON.stringify(command),
                    },
                ],
                topic: commandTopic,
            };
            const clientId = 'Orchestrator_test_stop_message';
            let adminClient: Admin;
            let producer: Producer;
            connectAdminClient(kafkaConfig(clientId))
                .pipe(
                    tap((_adminClient) => (adminClient = _adminClient)),
                    concatMap(() => deleteTopics(adminClient, [commandTopic])),
                    concatMap(() => connectProducer(kafkaConfig(clientId))),
                    tap((_producer) => (producer = _producer)),
                    delay(10000), // delay to increase the possibility that the Doer is up when we send the END command
                    concatMap(() => producer.send(endCommandRecord)),
                    concatMap(() => orchestrator.doerMessage$),
                    find((doerMessage) => doerMessage.kafkaMessage.value.toString() === 'Stop'),
                )
                .subscribe({
                    error: (e) => {
                        producer.disconnect();
                        adminClient.disconnect();
                        orchestrator.disconnect();
                        done(e);
                    },
                    complete: () => {
                        producer.disconnect();
                        adminClient.disconnect();
                        orchestrator.disconnect();
                        done();
                    },
                });
        }).timeout(100000);
    });
});
