import 'mocha';
import { expect } from 'chai';

import { join } from 'path';
import { readConfigurationFromFile } from './configuration';

describe('readConfigurationFromFile function', () => {
    it('reads the config data from a local file and creates an array of Configuration objects', done => {
        const fileDir = './';
        const fileName = 'configuration.file.json';
        const filePath = join(__dirname, fileName);
        console.log(__dirname);
        console.log(filePath);
        readConfigurationFromFile(filePath).subscribe(
            configurations => {
                expect(configurations.length).to.equal(2);
            },
            err => {
                console.error('ERROR', err);
                done(err);
            },
            () => done(),
        );
    });
});
