import { Client, Connection } from '@temporalio/client';
import { randomUUID } from 'crypto';
import { greetingWorkflow } from './workflows';
import { TASK_QUEUE } from './worker';

async function run(): Promise<void> {
  const connection = await Connection.connect({
    address: process.env.TEMPORAL_ADDRESS ?? 'localhost:7233',
  });
  const client = new Client({
    connection,
    namespace: process.env.TEMPORAL_NAMESPACE ?? 'default',
  });

  const result = await client.workflow.execute(greetingWorkflow, {
    workflowId: `greeting-typescript-${randomUUID()}`,
    taskQueue: TASK_QUEUE,
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
