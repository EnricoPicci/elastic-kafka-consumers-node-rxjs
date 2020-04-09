import { ElasticConsumer } from '../../elastic-consumer/elastic-consumer';
import { KafkaMessage } from 'kafkajs';
import { of, Observable } from 'rxjs';

export class DoConsumer extends ElasticConsumer<string> {
    // the default doer function provides a default implementation of do logic
    doer: (message: KafkaMessage) => Observable<any> = (message: KafkaMessage) =>
        of(`Nothing done with essage ${message}`);
    processMessage(message: KafkaMessage) {
        return this.doer(message);
    }
}
