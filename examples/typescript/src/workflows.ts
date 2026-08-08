import { proxyActivities } from '@temporalio/workflow';
import type * as activities from './activities';

const { composeGreeting } = proxyActivities<typeof activities>({
  startToCloseTimeout: '10 seconds',
});

export async function greetingWorkflow(name: string): Promise<string> {
  return await composeGreeting(name);
}
