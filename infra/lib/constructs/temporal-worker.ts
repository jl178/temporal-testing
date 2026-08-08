import {
  RemovalPolicy,
  aws_ec2 as ec2,
  aws_ecs as ecs,
  aws_logs as logs,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';

export interface TemporalWorkerServiceProps {
  readonly ecsCluster: ecs.ICluster;
  /** Worker container image (e.g. built from one of the examples/ dirs). */
  readonly image: ecs.ContainerImage;
  /** Temporal frontend address, e.g. `temporal-frontend.temporal.local:7233`. */
  readonly temporalAddress: string;
  /** @default 256 */
  readonly cpu?: number;
  /** @default 512 */
  readonly memoryLimitMiB?: number;
  /** @default 1 */
  readonly desiredCount?: number;
  /** Extra container environment. */
  readonly environment?: Record<string, string>;
}

/**
 * Optional reusable construct for running a containerized Temporal worker on
 * Fargate, wired to the cluster via TEMPORAL_ADDRESS. Not deployed by the
 * default app — workers in this repo run locally against Docker Compose.
 */
export class TemporalWorkerService extends Construct {
  public readonly service: ecs.FargateService;

  constructor(scope: Construct, id: string, props: TemporalWorkerServiceProps) {
    super(scope, id);

    const logGroup = new logs.LogGroup(this, 'Logs', {
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const taskDefinition = new ecs.FargateTaskDefinition(this, 'Task', {
      cpu: props.cpu ?? 256,
      memoryLimitMiB: props.memoryLimitMiB ?? 512,
    });
    taskDefinition.addContainer('worker', {
      image: props.image,
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'temporal-worker', logGroup }),
      environment: {
        TEMPORAL_ADDRESS: props.temporalAddress,
        ...props.environment,
      },
    });

    this.service = new ecs.FargateService(this, 'Service', {
      cluster: props.ecsCluster,
      taskDefinition,
      desiredCount: props.desiredCount ?? 1,
      minHealthyPercent: 0,
    });
  }

  /** Allow this worker to reach the Temporal frontend. */
  public allowGrpcTo(server: ec2.IConnectable, port = 7233): void {
    server.connections.allowFrom(this.service, ec2.Port.tcp(port), 'Worker to Temporal frontend');
  }
}
