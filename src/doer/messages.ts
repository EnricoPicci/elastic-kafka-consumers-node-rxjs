import { ProducerRecord, Message } from 'kafkajs';

export type MessageId = 'Start' | 'Stop' | 'Error';

export type MessageRecord = {
    message: [];
    topic;
};

export function newMessage(id: MessageId): Message {
    return { value: id };
}

export function newMessageRecord(id: MessageId, topic: string) {
    const record: ProducerRecord = {
        messages: [newMessage(id)],
        topic,
    };
    return record;
}
