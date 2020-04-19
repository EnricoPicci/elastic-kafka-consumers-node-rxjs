import 'mocha';
import { expect } from 'chai';

import { join } from 'path';
import { readConfigFromFile } from './config';

describe('readConfigFromFile function', () => {
    it('reads the config data from a local file and creates an array of Configuration objects', (done) => {
        const fileName = 'config.file.json';
        const filePath = join(__dirname, fileName);
        readConfigFromFile(filePath).subscribe(
            (config) => {
                expect(config).to.be.not.undefined;
                expect(config.topics.length).to.equal(2);
                expect(config.doers.length).to.equal(2);
            },
            (err) => {
                console.error('ERROR', err);
                done(err);
            },
            () => done(),
        );
    });
});
