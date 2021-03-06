import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { readLinesObs } from 'observable-fs';

export type Config = {
    topics: TopicConfig[];
    doers: DoerConfig[];
};
export type TopicConfig = {
    name: string;
    partitions: number;
};
export type DoerConfig = {
    name: string;
    inputTopic: string;
    outputTopics?: string[];
    function: string;
    functionModule: string;
    concurrency?: string;
};

export function readConfigFromFile(filePath: string): Observable<Config> {
    return readLinesObs(filePath).pipe(
        map((lines) => lines.join('')),
        map((rawData) => JSON.parse(rawData)),
    );
}
