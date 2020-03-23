import { KafkaMessage } from 'kafkajs';
import { Observable } from 'rxjs';

import { ConfigurationRecord } from '../orchestrator/configuration/configuration';

abstract class Consumer {
    constructor(private name: string, private configuration: ConfigurationRecord) {}

    abstract processMessage<T>(message: KafkaMessage): Observable<T>;

    start() {}
}
