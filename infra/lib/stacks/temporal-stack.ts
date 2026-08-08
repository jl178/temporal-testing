import {
  CfnOutput,
  Stack,
  StackProps,
  aws_ec2 as ec2,
  aws_ecs as ecs,
  aws_route53 as route53,
  aws_secretsmanager as secretsmanager,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { TemporalCluster } from '../constructs/temporal-cluster';
import { TemporalDatabase } from '../constructs/temporal-database';
import { TemporalDns } from '../constructs/temporal-dns';

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
  /** Cloud Map service discovery for the server. @default true */
  readonly serviceDiscovery?: boolean;
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
      serviceDiscovery: props.serviceDiscovery,
    });

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
