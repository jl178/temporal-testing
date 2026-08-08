import { aws_ec2 as ec2 } from 'aws-cdk-lib';
import { Construct } from 'constructs';

export interface TemporalNetworkProps {
  /**
   * Existing VPC to run Temporal in. When omitted, a new 2-AZ VPC with one
   * NAT gateway is created.
   */
  readonly vpc?: ec2.IVpc;
  /** Only used when creating a VPC. @default 2 */
  readonly maxAzs?: number;
  /** Only used when creating a VPC. @default 1 */
  readonly natGateways?: number;
}

/**
 * Network layer for a Temporal deployment: wraps an existing VPC when one is
 * supplied, otherwise creates a minimal VPC suitable for Fargate + Aurora.
 */
export class TemporalNetwork extends Construct {
  public readonly vpc: ec2.IVpc;
  /** True when this construct created the VPC (vs importing an existing one). */
  public readonly createdVpc: boolean;

  constructor(scope: Construct, id: string, props: TemporalNetworkProps = {}) {
    super(scope, id);

    if (props.vpc) {
      this.vpc = props.vpc;
      this.createdVpc = false;
    } else {
      this.vpc = new ec2.Vpc(this, 'Vpc', {
        maxAzs: props.maxAzs ?? 2,
        natGateways: props.natGateways ?? 1,
        subnetConfiguration: [
          { name: 'public', subnetType: ec2.SubnetType.PUBLIC, cidrMask: 24 },
          { name: 'private', subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS, cidrMask: 24 },
        ],
      });
      this.createdVpc = true;
    }
  }
}
