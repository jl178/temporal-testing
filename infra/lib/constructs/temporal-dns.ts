import {
  aws_elasticloadbalancingv2 as elbv2,
  aws_route53 as route53,
  aws_route53_targets as targets,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';

export interface TemporalDnsProps {
  /** Domain the records live under, e.g. `temporal.example.com`. */
  readonly domainName: string;
  /**
   * Existing hosted zone to add records to. When omitted, a public hosted
   * zone is created for `domainName`.
   */
  readonly hostedZone?: route53.IHostedZone;
}

/**
 * Optional DNS layer: import-or-create a hosted zone, then alias
 * `ui.<domain>` at the UI ALB and `grpc.<domain>` at the gRPC NLB.
 */
export class TemporalDns extends Construct {
  public readonly hostedZone: route53.IHostedZone;
  public readonly createdZone: boolean;
  public readonly uiDomainName: string;
  public readonly grpcDomainName: string;

  constructor(scope: Construct, id: string, props: TemporalDnsProps) {
    super(scope, id);

    if (props.hostedZone) {
      this.hostedZone = props.hostedZone;
      this.createdZone = false;
    } else {
      this.hostedZone = new route53.PublicHostedZone(this, 'Zone', {
        zoneName: props.domainName,
      });
      this.createdZone = true;
    }
    this.uiDomainName = `ui.${props.domainName}`;
    this.grpcDomainName = `grpc.${props.domainName}`;
  }

  public addUiRecord(loadBalancer: elbv2.IApplicationLoadBalancer): route53.ARecord {
    return new route53.ARecord(this, 'UiAlias', {
      zone: this.hostedZone,
      recordName: this.uiDomainName,
      target: route53.RecordTarget.fromAlias(new targets.LoadBalancerTarget(loadBalancer)),
    });
  }

  public addGrpcRecord(loadBalancer: elbv2.INetworkLoadBalancer): route53.ARecord {
    return new route53.ARecord(this, 'GrpcAlias', {
      zone: this.hostedZone,
      recordName: this.grpcDomainName,
      target: route53.RecordTarget.fromAlias(new targets.LoadBalancerTarget(loadBalancer)),
    });
  }
}
