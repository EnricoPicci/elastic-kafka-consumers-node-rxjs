import 'mocha';
import { expect } from 'chai';

import { join } from 'path';
import { countWords } from './count-words';

describe(`When a file is read`, () => {
    it(`the occurrencies of each word are counted`, (done) => {
        const file = join(__dirname, 'count-words.test-file.txt');
        countWords(file).subscribe({
            next: (wordCounts) => {
                expect(Object.keys(wordCounts).length).to.be.gt(0);
                expect(wordCounts['non']).to.equal(3);
            },
            complete: () => done(),
        });
    });
});
