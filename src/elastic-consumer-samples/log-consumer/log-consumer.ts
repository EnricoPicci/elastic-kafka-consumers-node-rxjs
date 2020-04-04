import { ElasticConsumer } from '../../elastic-consumer/elastic-consumer';
import { KafkaMessage } from 'kafkajs';

export class LogConsumer extends ElasticConsumer<string> {
    logger: (message: KafkaMessage) => void = console.log;

    processMessage(message: KafkaMessage) {
        this.logger(message);
        return message.value.toString();
    }
}
