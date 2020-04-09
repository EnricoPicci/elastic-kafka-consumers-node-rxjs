import { ElasticConsumer } from '../../elastic-consumer/elastic-consumer';
import { KafkaMessage } from 'kafkajs';
import { of } from 'rxjs';

export class LogConsumer extends ElasticConsumer<string> {
    logger: (message: KafkaMessage) => void = console.log;

    processMessage(message: KafkaMessage) {
        this.logger(message);
        return of(message.value.toString());
    }
}
