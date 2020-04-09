import { ElasticConsumer } from '../../elastic-consumer/elastic-consumer';
import { KafkaMessage } from 'kafkajs';
import { of, Observable } from 'rxjs';

export class LogConsumer extends ElasticConsumer<string> {
    logger: (message: KafkaMessage) => Observable<any> = (message: KafkaMessage) => {
        console.log(message);
        return of(`Message ${message} logged`);
    };

    processMessage(message: KafkaMessage) {
        this.logger(message);
        return of(message.value.toString());
    }
}
