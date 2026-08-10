import {
  CfnOutput,
  RemovalPolicy,
  Stack,
  StackProps,
  aws_emrserverless as emrserverless,
  aws_iam as iam,
  aws_s3 as s3,
  aws_transfer as transfer,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';

export interface DataPlaneStackProps extends StackProps {
  /**
   * EMR Serverless custom image URI (ECR) with dbt installed. Must be
   * pushed BEFORE deploy — EMR validates the image when the application
   * is created. Omit for the stock runtime (no dbt: runner jobs that
   * invoke dbt will fail).
   */
  readonly emrImageUri?: string;
  /**
   * SSH public key for the SFTP user. When set, an AWS Transfer Family
   * SFTP server is created whose `etl` user lands files in the data
   * bucket's landing/ prefix — the prod binding of the local SFTP
   * container. Omit to skip Transfer (it bills hourly).
   */
  readonly sftpPublicKey?: string;
}

/**
 * The ETL data plane, prod bindings of the local emulators (docs/decisions
 * D2): S3 data lake bucket ⇄ LocalEmu S3, EMR Serverless application ⇄
 * Spark containers, Glue Data Catalog ⇄ Iceberg REST, Transfer Family ⇄
 * SFTP container. Everything is created (never imported) and destroyable.
 */
export class DataPlaneStack extends Stack {
  public readonly bucket: s3.Bucket;
  public readonly emrApp: emrserverless.CfnApplication;
  public readonly emrExecutionRole: iam.Role;

  constructor(scope: Construct, id: string, props: DataPlaneStackProps = {}) {
    super(scope, id, props);

    this.bucket = new s3.Bucket(this, 'DataLake', {
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
    });

    // The role EMR jobs run as: the data lake + the Glue catalog databases
    // the dbt project materializes into (raw = landed inputs, analytics =
    // marts, default = Spark's fallback namespace).
    this.emrExecutionRole = new iam.Role(this, 'EmrExecutionRole', {
      assumedBy: new iam.ServicePrincipal('emr-serverless.amazonaws.com'),
    });
    this.bucket.grantReadWrite(this.emrExecutionRole);
    const glueDbs = ['raw', 'analytics', 'default'];
    this.emrExecutionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          'glue:GetDatabase', 'glue:GetDatabases', 'glue:CreateDatabase',
          'glue:GetTable', 'glue:GetTables', 'glue:CreateTable',
          'glue:UpdateTable', 'glue:DeleteTable',
          'glue:GetPartition', 'glue:GetPartitions', 'glue:BatchCreatePartition',
          'glue:BatchDeletePartition', 'glue:BatchGetPartition', 'glue:UpdatePartition',
        ],
        resources: [
          `arn:aws:glue:${this.region}:${this.account}:catalog`,
          ...glueDbs.map((d) => `arn:aws:glue:${this.region}:${this.account}:database/${d}`),
          ...glueDbs.map((d) => `arn:aws:glue:${this.region}:${this.account}:table/${d}/*`),
        ],
      }),
    );

    this.emrApp = new emrserverless.CfnApplication(this, 'EmrApp', {
      name: this.stackName,
      type: 'SPARK',
      releaseLabel: 'emr-7.9.0',
      ...(props.emrImageUri
        ? { imageConfiguration: { imageUri: props.emrImageUri } }
        : {}),
      autoStopConfiguration: { enabled: true, idleTimeoutMinutes: 5 },
      maximumCapacity: { cpu: '16 vCPU', memory: '64 GB' },
    });

    if (props.sftpPublicKey) {
      const transferRole = new iam.Role(this, 'SftpUserRole', {
        assumedBy: new iam.ServicePrincipal('transfer.amazonaws.com'),
      });
      // Transfer scopes the session to the user's home directory; the role
      // needs the underlying bucket permissions for that prefix.
      transferRole.addToPolicy(
        new iam.PolicyStatement({
          actions: ['s3:ListBucket', 's3:GetBucketLocation'],
          resources: [this.bucket.bucketArn],
        }),
      );
      transferRole.addToPolicy(
        new iam.PolicyStatement({
          actions: ['s3:PutObject', 's3:GetObject', 's3:DeleteObject'],
          resources: [this.bucket.arnForObjects('landing/*')],
        }),
      );
      const server = new transfer.CfnServer(this, 'Sftp', {
        protocols: ['SFTP'],
        domain: 'S3',
        identityProviderType: 'SERVICE_MANAGED',
        endpointType: 'PUBLIC',
      });
      new transfer.CfnUser(this, 'SftpUser', {
        serverId: server.attrServerId,
        userName: 'etl',
        role: transferRole.roleArn,
        homeDirectory: `/${this.bucket.bucketName}/landing`,
        sshPublicKeys: [props.sftpPublicKey],
      });
      new CfnOutput(this, 'SftpEndpoint', {
        value: `${server.attrServerId}.server.transfer.${this.region}.amazonaws.com`,
      });
      new CfnOutput(this, 'SftpUserName', { value: 'etl' });
    }

    new CfnOutput(this, 'DataBucket', { value: this.bucket.bucketName });
    new CfnOutput(this, 'EmrApplicationId', { value: this.emrApp.attrApplicationId });
    new CfnOutput(this, 'EmrExecutionRoleArn', { value: this.emrExecutionRole.roleArn });
  }
}
