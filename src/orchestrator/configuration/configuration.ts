import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { readLinesObs } from 'observable-fs';

export type ConfigurationRecord = {
    consumerGroup: string;
    maxConsumers: number;
    topic: string;
};

export function readConfigurationFromFile(filePath: string): Observable<ConfigurationRecord[]> {
    return readLinesObs(filePath).pipe(
        map(lines => lines.join('')),
        map(rawData => JSON.parse(rawData)),
    );
}
