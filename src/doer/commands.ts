export type CommandId = 'END' | 'CHANGE_CONCURRENCY';

export type Command = {
    commandId: CommandId;
};

export function parseCommand(commandJson: string): Command {
    return JSON.parse(commandJson);
}
