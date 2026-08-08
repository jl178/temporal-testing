import { NativeConnection, Worker } from '@temporalio/worker';
import * as activities from './activities';

export const TASK_QUEUE = 'greeting-tasks-typescript';

async function run(): Promise<void> {
  const connection = await NativeConnection.connect({
    address: process.env.TEMPORAL_ADDRESS ?? 'localhost:7233',
  });
  const worker = await Worker.create({
    connection,
    namespace: 'default',
    taskQueue: TASK_QUEUE,
    workflowsPath: require.resolve('./workflows'),
    activities,
  });
  console.log(`Worker listening on task queue '${TASK_QUEUE}'`);
  await worker.run();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
