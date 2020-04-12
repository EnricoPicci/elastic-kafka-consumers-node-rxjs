import { ElasticConsumer } from '../../elastic-consumer/elastic-consumer';
import { KafkaMessage } from 'kafkajs';
import { Observable } from 'rxjs';
import { countWords } from './count-words';

export class WordCounterConsumer extends ElasticConsumer<{ [word: string]: number }> {
    processMessage(message: KafkaMessage): Observable<{ [word: string]: number }> {
        const file = message.value.toString();
        return countWords(file);
    }
}
