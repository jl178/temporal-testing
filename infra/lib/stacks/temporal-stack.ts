import {
  CfnOutput,
  RemovalPolicy,
  Stack,
  StackProps,
  aws_ec2 as ec2,
  aws_ecr as ecr,
  aws_ecs as ecs,
  aws_iam as iam,
  aws_logs as logs,
  aws_route53 as route53,
  aws_secretsmanager as secretsmanager,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { TemporalCluster } from '../constructs/temporal-cluster';
import { TemporalDatabase } from '../constructs/temporal-database';
import { TemporalDns } from '../constructs/temporal-dns';
import { TemporalWorkerService } from '../constructs/temporal-worker';
import { DataPlaneStack } from './data-plane-stack';

export interface ExistingDatabaseConfig {
  readonly endpointAddress: string;
  readonly port?: number;
  readonly secretArn: string;
  readonly securityGroupId?: string;
}

export interface ExistingHostedZoneConfig {
  readonly hostedZoneId: string;
  readonly zoneName: string;
}

export interface TemporalStackProps extends StackProps {
  readonly vpc: ec2.IVpc;
  /** Name of an existing ECS cluster to deploy into. Omit to create one. */
  readonly ecsClusterName?: string;
  /** Existing Postgres database. Omit to create Aurora Serverless v2. */
  readonly existingDatabase?: ExistingDatabaseConfig;
  /** Enables DNS: records are created under this domain. Omit to skip DNS. */
  readonly domainName?: string;
  /** Existing hosted zone for domainName. Omit to create a zone (when domainName is set). */
  readonly existingHostedZone?: ExistingHostedZoneConfig;
  /** @default true */
  readonly publicUi?: boolean;
  /** Public-UI ingress allowlist; see TemporalClusterProps.uiAllowedCidrs. */
  readonly uiAllowedCidrs?: string[];
  /** Cloud Map service discovery for the server. @default true */
  readonly serviceDiscovery?: boolean;
  /** See TemporalDatabaseProps.auroraVersion. */
  readonly auroraVersion?: string;
  /**
   * Workflow-execution e2e harness: a worker fleet + a one-shot starter
   * task definition using a pre-pushed image from ECR. Used by the
   * aws-deploy-validate workflow; omit for normal deployments.
   */
  readonly e2eWorker?: { repoName: string; tag: string };
  /**
   * ETL fleets + starter wired to a DataPlaneStack (S3/EMR/Glue): the
   * aws-data-validate workflow's harness. Omit for normal deployments.
   */
  readonly etlWorker?: { repoName: string; tag: string; dataPlane: DataPlaneStack };
}

/**
 * The full Temporal deployment: database (create or import), server + UI on
 * Fargate (cluster create or import), and optional DNS (zone create or import).
 */
export class TemporalStack extends Stack {
  public readonly database: TemporalDatabase;
  public readonly temporal: TemporalCluster;
  public readonly dns?: TemporalDns;

  constructor(scope: Construct, id: string, props: TemporalStackProps) {
    super(scope, id, props);

    this.database = new TemporalDatabase(this, 'Database', {
      vpc: props.vpc,
      auroraVersion: props.auroraVersion,
      existingDatabase: props.existingDatabase
        ? {
            endpointAddress: props.existingDatabase.endpointAddress,
            port: props.existingDatabase.port,
            secret: secretsmanager.Secret.fromSecretCompleteArn(
              this,
              'ImportedDbSecret',
              props.existingDatabase.secretArn,
            ),
            securityGroup: props.existingDatabase.securityGroupId
              ? ec2.SecurityGroup.fromSecurityGroupId(
                  this,
                  'ImportedDbSg',
                  props.existingDatabase.securityGroupId,
                )
              : undefined,
          }
        : undefined,
    });

    this.temporal = new TemporalCluster(this, 'Temporal', {
      vpc: props.vpc,
      ecsCluster: props.ecsClusterName
        ? ecs.Cluster.fromClusterAttributes(this, 'ImportedEcsCluster', {
            clusterName: props.ecsClusterName,
            vpc: props.vpc,
          })
        : undefined,
      database: this.database,
      publicUi: props.publicUi,
      uiAllowedCidrs: props.uiAllowedCidrs,
      serviceDiscovery: props.serviceDiscovery,
    });

    // D15 topology: worker fleets live in a separate WORKLOAD cluster —
    // the platform cluster (server + UI) is the platform team's; on
    // Fargate a cluster is a free namespace, so the boundary costs nothing.
    const workloadCluster =
      props.e2eWorker || props.etlWorker
        ? new ecs.Cluster(this, 'WorkloadCluster', {
            vpc: props.vpc,
            clusterName: `${this.stackName}-workload`,
          })
        : undefined;

    if (props.domainName) {
      this.dns = new TemporalDns(this, 'Dns', {
        domainName: props.domainName,
        hostedZone: props.existingHostedZone
          ? route53.HostedZone.fromHostedZoneAttributes(this, 'ImportedZone', {
              hostedZoneId: props.existingHostedZone.hostedZoneId,
              zoneName: props.existingHostedZone.zoneName,
            })
          : undefined,
      });
      this.dns.addUiRecord(this.temporal.uiService.loadBalancer);
      this.dns.addGrpcRecord(this.temporal.grpcLoadBalancer);
    }

    if (props.e2eWorker) {
      const repo = ecr.Repository.fromRepositoryName(
        this, 'E2eRepo', props.e2eWorker.repoName,
      );
      const image = ecs.ContainerImage.fromEcrRepository(repo, props.e2eWorker.tag);

      const worker = new TemporalWorkerService(this, 'E2eWorker', {
        ecsCluster: workloadCluster!,
        image,
        temporalAddress: this.temporal.grpcEndpoint,
        taskQueue: 'greeting-tasks-python',
        profile: 'medium',
        // Slot-capped slow activities: burst starts outpace one worker so a
        // real backlog forms and the autoscaler has something to do.
        environment: {
          GREET_DELAY_SECONDS: '3',
          WORKER_MAX_ACTIVITIES: '4',
        },
        autoscaling: {
          minCapacity: 1,
          maxCapacity: 3,
          scaleUpBacklog: 10,
          temporalHttpEndpoint: this.temporal.httpApiEndpoint,
        },
      });
      worker.allowGrpcTo(this.temporal.serverService);

      const starterLogs = new logs.LogGroup(this, 'E2eStarterLogs', {
        logGroupName: `/ecs/${this.stackName}/e2e-starter`,
        retention: logs.RetentionDays.ONE_DAY,
        removalPolicy: RemovalPolicy.DESTROY,
      });
      const starterTask = new ecs.FargateTaskDefinition(this, 'E2eStarterTask', {
        family: `${this.stackName}-e2e-starter`,
        cpu: 256,
        memoryLimitMiB: 512,
      });
      starterTask.addContainer('starter', {
        image,
        command: ['python', 'starter.py'],
        environment: {
          TEMPORAL_ADDRESS: this.temporal.grpcEndpoint,
          TEMPORAL_NAMESPACE: 'default',
        },
        logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'e2e-starter', logGroup: starterLogs }),
      });
      const starterSg = new ec2.SecurityGroup(this, 'E2eStarterSg', {
        vpc: props.vpc,
        allowAllOutbound: true,
      });

      new CfnOutput(this, 'E2eClusterName', { value: workloadCluster!.clusterName });
      new CfnOutput(this, 'E2eStarterTaskDef', { value: starterTask.taskDefinitionArn });
      new CfnOutput(this, 'E2eStarterSecurityGroup', { value: starterSg.securityGroupId });
      new CfnOutput(this, 'E2ePrivateSubnets', {
        value: props.vpc.privateSubnets.map((s) => s.subnetId).join(','),
      });
      new CfnOutput(this, 'E2eStarterLogGroup', { value: starterLogs.logGroupName });
    }

    if (props.etlWorker) {
      const { dataPlane } = props.etlWorker;
      const repo = ecr.Repository.fromRepositoryName(
        this, 'EtlRepo', props.etlWorker.repoName,
      );
      const image = ecs.ContainerImage.fromEcrRepository(repo, props.etlWorker.tag);
      const dataEnv = {
        ETL_BUCKET: dataPlane.bucket.bucketName,
        GLUE_WAREHOUSE: `s3://${dataPlane.bucket.bucketName}/warehouse`,
        EMR_APPLICATION_ID: dataPlane.emrApp.attrApplicationId,
        EMR_EXECUTION_ROLE_ARN: dataPlane.emrExecutionRole.roleArn,
        // The EMR-bundled Iceberg runtime; spark_job.py adds the Glue
        // catalog session confs itself.
        EMR_SPARK_SUBMIT_PARAMS:
          '--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar',
      };

      // Invariant: workflow workers never register activities — two fleets.
      const wfFleet = new TemporalWorkerService(this, 'EtlWorkflowWorker', {
        ecsCluster: workloadCluster!,
        image,
        temporalAddress: this.temporal.grpcEndpoint,
        taskQueue: 'etl-pipeline',
        profile: 'xsmall',
        command: ['python', '-m', 'worker_platform', '--queue', 'etl-pipeline',
          '--profile', 'xsmall', '--workflows', 'workflow'],
      });
      const actFleet = new TemporalWorkerService(this, 'EtlActivityWorker', {
        ecsCluster: workloadCluster!,
        image,
        temporalAddress: this.temporal.grpcEndpoint,
        taskQueue: 'etl-pipeline',
        profile: 'small',
        command: ['python', '-m', 'worker_platform', '--queue', 'etl-pipeline',
          '--profile', 'small', '--activities', 'activities', '--activities', 'demo'],
        environment: dataEnv,
      });
      wfFleet.allowGrpcTo(this.temporal.serverService);
      actFleet.allowGrpcTo(this.temporal.serverService);

      // The in-process compute fallback lane at its documented size —
      // deploys the `large` Fargate mapping (4 vCPU / 16 GB) for real.
      const computeFleet = new TemporalWorkerService(this, 'EtlComputeLarge', {
        ecsCluster: workloadCluster!,
        image,
        temporalAddress: this.temporal.grpcEndpoint,
        taskQueue: 'compute-large',
        profile: 'large',
        command: ['python', '-m', 'worker_platform', '--queue', 'compute-large',
          '--profile', 'large', '--activities', 'activities:run_local_transform'],
        environment: dataEnv,
      });
      computeFleet.allowGrpcTo(this.temporal.serverService);

      // The activity fleet is the launcher: lake r/w, EMR submit/poll, and
      // handing the execution role to EMR (never assuming it itself).
      const actRole = actFleet.service.taskDefinition.taskRole;
      dataPlane.bucket.grantReadWrite(actRole);
      actRole.addToPrincipalPolicy(new iam.PolicyStatement({
        actions: [
          'emr-serverless:StartApplication', 'emr-serverless:GetApplication',
          'emr-serverless:StartJobRun', 'emr-serverless:GetJobRun',
        ],
        resources: [
          `arn:aws:emr-serverless:${this.region}:${this.account}:/applications/${dataPlane.emrApp.attrApplicationId}`,
          `arn:aws:emr-serverless:${this.region}:${this.account}:/applications/${dataPlane.emrApp.attrApplicationId}/jobruns/*`,
        ],
      }));
      actRole.addToPrincipalPolicy(new iam.PolicyStatement({
        actions: ['iam:PassRole'],
        resources: [dataPlane.emrExecutionRole.roleArn],
        conditions: {
          StringEquals: { 'iam:PassedToService': 'emr-serverless.amazonaws.com' },
        },
      }));

      const etlStarterLogs = new logs.LogGroup(this, 'EtlStarterLogs', {
        logGroupName: `/ecs/${this.stackName}/etl-starter`,
        retention: logs.RetentionDays.ONE_DAY,
        removalPolicy: RemovalPolicy.DESTROY,
      });
      const etlStarterTask = new ecs.FargateTaskDefinition(this, 'EtlStarterTask', {
        family: `${this.stackName}-etl-starter`,
        cpu: 256,
        memoryLimitMiB: 512,
      });
      etlStarterTask.addContainer('starter', {
        image,
        command: ['python', 'starter.py'],
        environment: {
          TEMPORAL_ADDRESS: this.temporal.grpcEndpoint,
          TEMPORAL_NAMESPACE: 'default',
          ...dataEnv,
        },
        logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'etl-starter', logGroup: etlStarterLogs }),
      });
      const etlStarterSg = new ec2.SecurityGroup(this, 'EtlStarterSg', {
        vpc: props.vpc,
        allowAllOutbound: true,
      });

      new CfnOutput(this, 'EtlClusterName', { value: workloadCluster!.clusterName });
      new CfnOutput(this, 'EtlStarterTaskDef', { value: etlStarterTask.taskDefinitionArn });
      new CfnOutput(this, 'EtlStarterSecurityGroup', { value: etlStarterSg.securityGroupId });
      new CfnOutput(this, 'EtlPrivateSubnets', {
        value: props.vpc.privateSubnets.map((s) => s.subnetId).join(','),
      });
      new CfnOutput(this, 'EtlStarterLogGroup', { value: etlStarterLogs.logGroupName });
    }

    new CfnOutput(this, 'GrpcEndpoint', {
      value: this.dns ? `${this.dns.grpcDomainName}:7233` : this.temporal.grpcEndpoint,
      description: 'Temporal frontend gRPC endpoint (reachable from inside the VPC)',
    });
    new CfnOutput(this, 'UiUrl', {
      value: this.dns ? `http://${this.dns.uiDomainName}` : this.temporal.uiUrl,
      description: 'Temporal web UI',
    });
    new CfnOutput(this, 'DatabaseSecretArn', {
      value: this.database.secret.secretArn,
      description: 'Secrets Manager secret holding database credentials',
    });
  }
}
