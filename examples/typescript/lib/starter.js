"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const client_1 = require("@temporalio/client");
const crypto_1 = require("crypto");
const workflows_1 = require("./workflows");
const worker_1 = require("./worker");
async function run() {
    const connection = await client_1.Connection.connect({
        address: process.env.TEMPORAL_ADDRESS ?? 'localhost:7233',
    });
    const client = new client_1.Client({ connection });
    const result = await client.workflow.execute(workflows_1.greetingWorkflow, {
        workflowId: `greeting-typescript-${(0, crypto_1.randomUUID)()}`,
        taskQueue: worker_1.TASK_QUEUE,
        args: ['Temporal'],
    });
    console.log(`Workflow result: ${result}`);
    if (result !== 'Hello, Temporal!') {
        throw new Error(`unexpected result: ${result}`);
    }
    console.log('TYPESCRIPT EXAMPLE: PASS');
    process.exit(0);
}
run().catch((err) => {
    console.error(err);
    process.exit(1);
});
