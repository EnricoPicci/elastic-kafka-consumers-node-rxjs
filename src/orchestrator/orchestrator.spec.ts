import 'mocha';
import { expect } from 'chai';
import { Orchestrator } from './orchestrator';
import { ProducerRecord, KafkaConfig, Producer, Admin } from 'kafkajs';
import { connectProducer, deleteTopics, connectAdminClient } from '../observable-kafkajs/observable-kafkajs';
import { testConfiguration } from '../observable-kafkajs/test-config';
import { tap, concatMap, take, find } from 'rxjs/operators';
import { EMPTY, of } from 'rxjs';

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

describe(`when an Orchestrator starts`, () => {
    // it(`it launches all doers specified in its config file and receives the Started message from them`, (done) => {
    //     const configFileName = 'orchestrator-test.config.json';
    //     const orchestrator = new Orchestrator();
    //     orchestrator.start(configFileName, 'localhost:9092');
    //     done();
    // });
    it(`it launches just one doer and sends it a message`, (done) => {
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
