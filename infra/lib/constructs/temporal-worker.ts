import {
  Duration,
  RemovalPolicy,
  aws_applicationautoscaling as appscaling,
  aws_cloudwatch as cloudwatch,
  aws_ec2 as ec2,
  aws_ecs as ecs,
  aws_events as events,
  aws_events_targets as targets,
  aws_iam as iam,
  aws_lambda as lambda,
  aws_logs as logs,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';

export interface WorkerAutoscalingProps {
  /** @default 1 */
  readonly minCapacity?: number;
  readonly maxCapacity: number;
  /**
   * Backlog depth that adds one worker task; 4x this depth adds three.
   * @default 100
   */
  readonly scaleUpBacklog?: number;
  /**
   * Temporal HTTP API endpoint the backlog poller queries, e.g.
   * TemporalCluster.httpApiEndpoint.
   */
  readonly temporalHttpEndpoint: string;
  /** @default Temporal/TaskQueue */
  readonly metricNamespace?: string;
}

/**
 * Fargate task sizing matching the worker_platform size profiles — the same
 * profile string a fleet passes to `python -m worker_platform --profile`
 * sizes its container here, so the concurrency envelope and the compute
 * envelope always agree.
 */
export const WORKER_PROFILE_SIZES: Record<string, { cpu: number; memoryLimitMiB: number }> = {
  small: { cpu: 256, memoryLimitMiB: 512 },
  medium: { cpu: 512, memoryLimitMiB: 1024 },
  large: { cpu: 4096, memoryLimitMiB: 16384 },
};

export interface TemporalWorkerServiceProps {
  readonly ecsCluster: ecs.ICluster;
  /** Worker container image (the team's own image — its code, its deps). */
  readonly image: ecs.ContainerImage;
  /**
   * Container command — typically the worker_platform invocation, e.g.
   * ['python', '-m', 'worker_platform', '--queue', 'billing-render',
   *  '--profile', 'large', '--activities', 'billing.render'].
   * Omit to use the image's CMD.
   */
  readonly command?: string[];
  /** Temporal frontend address, e.g. `temporal-frontend.temporal.local:7233`. */
  readonly temporalAddress: string;
  /** Task queue this fleet polls — also the autoscaling dimension. */
  readonly taskQueue: string;
  /** @default default */
  readonly temporalNamespace?: string;
  /**
   * Size profile: sets Fargate cpu/memory to match the in-process
   * worker_platform profile. Explicit cpu/memoryLimitMiB override it.
   * @default small
   */
  readonly profile?: 'small' | 'medium' | 'large';
  /** @default from profile */
  readonly cpu?: number;
  /** @default from profile */
  readonly memoryLimitMiB?: number;
  /** @default 1 */
  readonly desiredCount?: number;
  /** Extra container environment. */
  readonly environment?: Record<string, string>;
  /**
   * Scale the fleet on task-queue backlog. A 1-minute EventBridge-driven
   * Lambda polls Temporal's DescribeTaskQueue HTTP API (enhanced mode
   * backlog statistics), publishes ApproximateBacklogCount /
   * ApproximateBacklogAgeSeconds to CloudWatch, and the service
   * step-scales on backlog depth. Omit for a fixed-size fleet.
   */
  readonly autoscaling?: WorkerAutoscalingProps;
}

/**
 * A Temporal worker fleet on Fargate, optionally autoscaled on its task
 * queue's backlog — the deployment-side answer to "scale out when
 * schedule-to-start latency climbs".
 */
export class TemporalWorkerService extends Construct {
  public readonly service: ecs.FargateService;
  /** Set when autoscaling is enabled. */
  public readonly backlogMetric?: cloudwatch.Metric;

  constructor(scope: Construct, id: string, props: TemporalWorkerServiceProps) {
    super(scope, id);

    const namespace = props.temporalNamespace ?? 'default';

    const logGroup = new logs.LogGroup(this, 'Logs', {
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const size = WORKER_PROFILE_SIZES[props.profile ?? 'small'];
    const taskDefinition = new ecs.FargateTaskDefinition(this, 'Task', {
      cpu: props.cpu ?? size.cpu,
      memoryLimitMiB: props.memoryLimitMiB ?? size.memoryLimitMiB,
    });
    taskDefinition.addContainer('worker', {
      image: props.image,
      command: props.command,
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'temporal-worker', logGroup }),
      environment: {
        TEMPORAL_ADDRESS: props.temporalAddress,
        TEMPORAL_NAMESPACE: namespace,
        TEMPORAL_TASK_QUEUE: props.taskQueue,
        ...props.environment,
      },
    });

    this.service = new ecs.FargateService(this, 'Service', {
      cluster: props.ecsCluster,
      taskDefinition,
      desiredCount: props.desiredCount ?? 1,
      minHealthyPercent: 0,
    });

    if (props.autoscaling) {
      this.backlogMetric = this.addBacklogAutoscaling(props, props.autoscaling, namespace);
    }
  }

  /** Allow this worker to reach the Temporal frontend. */
  public allowGrpcTo(server: ec2.IConnectable, port = 7233): void {
    server.connections.allowFrom(this.service, ec2.Port.tcp(port), 'Worker to Temporal frontend');
  }

  private addBacklogAutoscaling(
    props: TemporalWorkerServiceProps,
    scaling: WorkerAutoscalingProps,
    namespace: string,
  ): cloudwatch.Metric {
    const metricNamespace = scaling.metricNamespace ?? 'Temporal/TaskQueue';
    const dimensions = { TaskQueue: props.taskQueue, TemporalNamespace: namespace };

    // Poller: DescribeTaskQueue (enhanced mode) -> CloudWatch. Uses the
    // Temporal HTTP API so the Lambda needs only the Python stdlib + boto3.
    const poller = new lambda.Function(this, 'BacklogPoller', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      timeout: Duration.seconds(30),
      vpc: props.ecsCluster.vpc,
      environment: {
        TEMPORAL_HTTP_ENDPOINT: scaling.temporalHttpEndpoint,
        TEMPORAL_NAMESPACE: namespace,
        TASK_QUEUE: props.taskQueue,
        METRIC_NAMESPACE: metricNamespace,
      },
      code: lambda.Code.fromInline(`
import json, os, urllib.parse, urllib.request
import boto3

cloudwatch = boto3.client("cloudwatch")

def handler(event, context):
    base = os.environ["TEMPORAL_HTTP_ENDPOINT"].rstrip("/")
    ns = os.environ["TEMPORAL_NAMESPACE"]
    task_queue = os.environ["TASK_QUEUE"]
    url = (
        f"{base}/api/v1/namespaces/{urllib.parse.quote(ns)}"
        f"/task-queues/{urllib.parse.quote(task_queue, safe='')}"
        "?apiMode=TASK_QUEUE_API_MODE_ENHANCED&reportStats=true"
        "&versions.allActive=true"
    )
    with urllib.request.urlopen(url, timeout=10) as response:
        data = json.load(response)

    backlog, oldest_age = 0, 0.0
    for version in (data.get("versionsInfo") or {}).values():
        for type_info in (version.get("typesInfo") or {}).values():
            stats = type_info.get("stats") or {}
            backlog += int(stats.get("approximateBacklogCount") or 0)
            age = str(stats.get("approximateBacklogAge") or "0s").rstrip("s")
            oldest_age = max(oldest_age, float(age or 0))

    dimensions = [
        {"Name": "TaskQueue", "Value": task_queue},
        {"Name": "TemporalNamespace", "Value": ns},
    ]
    cloudwatch.put_metric_data(
        Namespace=os.environ["METRIC_NAMESPACE"],
        MetricData=[
            {"MetricName": "ApproximateBacklogCount", "Dimensions": dimensions,
             "Value": backlog, "Unit": "Count"},
            {"MetricName": "ApproximateBacklogAgeSeconds", "Dimensions": dimensions,
             "Value": oldest_age, "Unit": "Seconds"},
        ],
    )
    return {"backlog": backlog, "oldest_age_seconds": oldest_age}
`),
    });
    poller.addToRolePolicy(
      new iam.PolicyStatement({ actions: ['cloudwatch:PutMetricData'], resources: ['*'] }),
    );
    new events.Rule(this, 'BacklogPollSchedule', {
      schedule: events.Schedule.rate(Duration.minutes(1)),
      targets: [new targets.LambdaFunction(poller)],
    });

    const backlogMetric = new cloudwatch.Metric({
      namespace: metricNamespace,
      metricName: 'ApproximateBacklogCount',
      dimensionsMap: dimensions,
      statistic: 'Maximum',
      period: Duration.minutes(1),
    });

    const scaleUp = scaling.scaleUpBacklog ?? 100;
    const capacity = this.service.autoScaleTaskCount({
      minCapacity: scaling.minCapacity ?? 1,
      maxCapacity: scaling.maxCapacity,
    });
    capacity.scaleOnMetric('BacklogScaling', {
      metric: backlogMetric,
      adjustmentType: appscaling.AdjustmentType.CHANGE_IN_CAPACITY,
      scalingSteps: [
        { upper: 0, change: -1 },            // empty queue: drain down
        { lower: scaleUp, change: +1 },      // backlog forming: add a worker
        { lower: scaleUp * 4, change: +3 },  // backlog runaway: add three
      ],
      cooldown: Duration.minutes(2),
    });
    return backlogMetric;
  }
}
