import { Stack, StackProps, aws_ec2 as ec2 } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { TemporalNetwork } from '../constructs/temporal-network';

export interface NetworkStackProps extends StackProps {
  /** Existing VPC id to import (requires account/region env). Omit to create one. */
  readonly vpcId?: string;
}

export class NetworkStack extends Stack {
  public readonly vpc: ec2.IVpc;

  constructor(scope: Construct, id: string, props: NetworkStackProps = {}) {
    super(scope, id, props);

    const network = new TemporalNetwork(this, 'Network', {
      vpc: props.vpcId
        ? ec2.Vpc.fromLookup(this, 'ImportedVpc', { vpcId: props.vpcId })
        : undefined,
    });
    this.vpc = network.vpc;
  }
}
