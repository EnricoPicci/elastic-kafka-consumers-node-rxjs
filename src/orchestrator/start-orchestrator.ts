// npm run tsc
// node ./dist/orchestrator/start-orchestrator configFilePath localhost:9092

import { Orchestrator } from './orchestrator';

function startOrchestrator() {
    new Orchestrator().start(process.argv[2], process.argv[3]);
}

startOrchestrator();
