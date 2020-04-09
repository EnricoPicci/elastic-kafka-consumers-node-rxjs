import { ElasticConsumer } from '../../elastic-consumer/elastic-consumer';
import { KafkaMessage } from 'kafkajs';
import { of } from 'rxjs';

export class FormatConsumer extends ElasticConsumer<string> {
    // the default formatter provides a default implementation of formatting logic
    formatter: (message: KafkaMessage) => string = (msg: KafkaMessage) =>
        `KafkaMessage  key: ${msg.key}  offset: ${msg.offset}  value: "${msg.value.toString()}"`;

    processMessage(message: KafkaMessage) {
        return of(this.formatter(message));
    }
}
