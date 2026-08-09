import {
  RemovalPolicy,
  aws_ec2 as ec2,
  aws_rds as rds,
  aws_secretsmanager as secretsmanager,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';

export interface ExistingDatabaseAttributes {
  /** Hostname of an existing PostgreSQL endpoint Temporal should use. */
  readonly endpointAddress: string;
  /** @default 5432 */
  readonly port?: number;
  /** Secret holding `username` and `password` JSON fields for the database. */
  readonly secret: secretsmanager.ISecret;
  /**
   * Security group protecting the existing database. When provided, ingress
   * from the Temporal server is granted automatically.
   */
  readonly securityGroup?: ec2.ISecurityGroup;
}

export interface TemporalDatabaseProps {
  readonly vpc: ec2.IVpc;
  /**
   * Attributes of an existing database. When omitted, an Aurora Serverless v2
   * PostgreSQL cluster is created with a generated master secret.
   */
  readonly existingDatabase?: ExistingDatabaseAttributes;
  /** ACUs, only used when creating the cluster. @default 0.5 */
  readonly minCapacity?: number;
  /** ACUs, only used when creating the cluster. @default 4 */
  readonly maxCapacity?: number;
  /** @default RemovalPolicy.DESTROY (this repo is an example, not production) */
  readonly removalPolicy?: RemovalPolicy;
}

/**
 * Persistence layer for Temporal: an Aurora Serverless v2 PostgreSQL cluster,
 * or a thin wrapper around existing database attributes when they are passed
 * in. Either way it exposes endpoint/port/secret plus an ingress helper.
 */
export class TemporalDatabase extends Construct {
  public readonly endpointAddress: string;
  public readonly port: number;
  public readonly secret: secretsmanager.ISecret;
  /** Set only when this construct created the cluster. */
  public readonly cluster?: rds.DatabaseCluster;
  private readonly existingSecurityGroup?: ec2.ISecurityGroup;

  constructor(scope: Construct, id: string, props: TemporalDatabaseProps) {
    super(scope, id);

    if (props.existingDatabase) {
      this.endpointAddress = props.existingDatabase.endpointAddress;
      this.port = props.existingDatabase.port ?? 5432;
      this.secret = props.existingDatabase.secret;
      this.existingSecurityGroup = props.existingDatabase.securityGroup;
    } else {
      // An explicit DatabaseSecret (rather than the cluster's attached secret)
      // so downstream Refs resolve to the secret ARN directly.
      const secret = new rds.DatabaseSecret(this, 'Secret', { username: 'temporal' });
      this.cluster = new rds.DatabaseCluster(this, 'Cluster', {
        engine: rds.DatabaseClusterEngine.auroraPostgres({
          // AWS retires old minors (16.4 no longer exists); pin a current
          // one explicitly rather than trusting CDK enum freshness.
          version: rds.AuroraPostgresEngineVersion.of('16.8', '16'),
        }),
        writer: rds.ClusterInstance.serverlessV2('Writer'),
        serverlessV2MinCapacity: props.minCapacity ?? 0.5,
        serverlessV2MaxCapacity: props.maxCapacity ?? 4,
        vpc: props.vpc,
        vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
        credentials: rds.Credentials.fromSecret(secret),
        defaultDatabaseName: 'temporal',
        removalPolicy: props.removalPolicy ?? RemovalPolicy.DESTROY,
      });
      this.endpointAddress = this.cluster.clusterEndpoint.hostname;
      this.port = 5432;
      this.secret = secret;
    }
  }

  /** Allow a peer (e.g. the Temporal server service) to reach the database. */
  public allowConnectionsFrom(peer: ec2.IConnectable): void {
    if (this.cluster) {
      this.cluster.connections.allowDefaultPortFrom(peer, 'Temporal server to database');
    } else if (this.existingSecurityGroup) {
      new ec2.Connections({
        securityGroups: [this.existingSecurityGroup],
        defaultPort: ec2.Port.tcp(this.port),
      }).allowDefaultPortFrom(peer, 'Temporal server to database');
    }
    // No cluster and no security group: caller manages database ingress themselves.
  }
}
