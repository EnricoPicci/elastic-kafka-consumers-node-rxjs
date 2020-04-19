import { Doer } from './doer';

export function startDoer(
    name: string,
    id: number,
    brokers: string[],
    inputTopic: string,
    consumerGroup: string,
    _concurrency: number,
    outputTopics: string[],
    _function: string,
    _functionModule: string,
    _cpuUsageCheckFrequency?: number,
    _cpuUsageThreshold?: number,
) {
    const doer = new Doer(
        name,
        id,
        brokers,
        inputTopic,
        consumerGroup,
        outputTopics,
        _concurrency,
        _cpuUsageCheckFrequency,
        _cpuUsageThreshold,
    );
    return import(_functionModule).then((_module) => {
        doer.do = _module[_function];
        doer.start();
        return doer;
    });
}
