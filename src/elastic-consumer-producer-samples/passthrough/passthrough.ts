import { ElasticConsumerProducer } from '../../elastic-consumer-producer/elastic-consumer-producer';
import { KafkaMessage } from 'kafkajs';
import { of } from 'rxjs';

export class Passthrough extends ElasticConsumerProducer<string> {
    logger: (message: KafkaMessage) => void = console.log;

    messageForProducer(result: string): string | Buffer {
        return result;
    }

    processMessage(message: KafkaMessage) {
        this.logger(message);
        return of(message.value.toString());
    }
}
