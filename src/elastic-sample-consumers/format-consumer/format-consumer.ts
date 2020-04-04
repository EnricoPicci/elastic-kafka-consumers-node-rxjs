import { ElasticConsumer } from '../../elastic-consumer/elastic-consumer';
import { KafkaMessage } from 'kafkajs';

export class FormatConsumer extends ElasticConsumer<string> {
    // the default formatter provides a default implementation of formatting logic
    formatter: (message: KafkaMessage) => string = (msg: KafkaMessage) =>
        `KafkaMessage  key: ${msg.key}  offset: ${msg.offset}  value: "${msg.value.toString()}"`;

    processMessage(message: KafkaMessage) {
        this.formatter(message);
        return message.value.toString();
    }
}
