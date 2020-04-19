import 'mocha';
import { expect } from 'chai';
import { startDoer } from './start-doer';

import { testConfiguration } from '../observable-kafkajs/test-config';
import { Doer } from './doer';
import { delay } from 'rxjs/operators';

describe(`When startDoer function is fired`, () => {
    it(`a Doer is created`, (done) => {
        let doer: Doer;
        startDoer(
            'TestDoer',
            0,
            testConfiguration.brokers,
            'inputTopic',
            'consumerGroup',
            1,
            ['outputTopic'],
            'doNothing',
            './test-functions',
        ).then((_doer) => {
            doer = _doer;
            console.log(doer);
            expect(doer).to.be.not.undefined;
            // delay introduced since when doer.groupIdJoined emits we are notsure that consumer has actually completed
            // it connection to the consumerGroup - without the delay it can happen that the doer.disconnect() gets fired
            // before the consumer has completed the connection to the consumerGroup and therefore the 'disconnection'
            // does not do its job and the test remains pending and the execution does not complete
            doer.groupIdJoined.pipe(delay(10)).subscribe(() => {
                doer.disconnect();
                done();
            });
        });
    }).timeout(10000);
});
