import { readLineObs } from 'observable-fs';
import { reduce } from 'rxjs/operators';

export function countWords(file: string) {
    return readLineObs(file).pipe(
        reduce<string, { [word: string]: number }>((acc, line, i: number) => {
            if (i % 100000 === 0) {
                const linesRead = new Intl.NumberFormat().format(i);
                console.log(`${linesRead} lines read`);
            }
            const words = line.replace(/[^A-Za-z0-9\s]/g, '').split(/\ +/);
            words
                .filter((word) => word !== '')
                .map((word) => word.toLowerCase())
                .forEach((word) => (acc[word] = acc[word] ? acc[word] + 1 : 1));
            return acc;
        }, {}),
    );
}
