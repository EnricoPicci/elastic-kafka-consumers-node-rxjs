import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { readLinesObs } from 'observable-fs';

export type Config = {
    brokers: string[];
    topics: TopicConfig[];
};
export type TopicConfig = {
    name: string;
    partitions: number;
};
export type DoConsumerConfig = {
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
