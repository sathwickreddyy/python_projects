import {RemovalPolicy, Stack, StackProps} from "aws-cdk-lib";
import {Construct} from "constructs";
import {SnsConstruct} from "./constructs/SNSConstruct";
import {RedisConstruct} from "./constructs/RedisConstruct";
import {SsmConstruct} from "./constructs/SSMConstruct";
import {Ec2Construct} from "./constructs/Ec2Construct";
import {LambdaConstruct} from "./constructs/LambdaConstuct";
import {Vpc} from "aws-cdk-lib/aws-ec2";
import * as s3 from 'aws-cdk-lib/aws-s3';


export interface LeaderElectionWithRedisStackProps extends StackProps {
    vpc: Vpc
}

export class LeaderElectionWithRedisStack extends Stack {
    constructor(scope: Construct, id: string, props: LeaderElectionWithRedisStackProps) {
    super(scope, id, props);

    // Create SNS Topic
    const snsConstruct = new SnsConstruct(this, 'SnsConstruct');

    // Create Redis (ElastiCache)
    const redisConstruct = new RedisConstruct(this, 'RedisConstruct', {
      vpc: props.vpc
    });

    // Create SSM Parameters for Redis and SNS configuration
    const ssmConstruct = new SsmConstruct(this, 'SsmConstruct', {
      redisEndpoint: redisConstruct.getRedisEndpoint(),
      snsTopicArn: snsConstruct.getTopicArn(),
    });

    // Create EC2 Instances with ALB and Auto Scaling Group
    const ec2Construct = new Ec2Construct(this, 'Ec2Construct', {
      vpc: props.vpc
    });

    // Create Lambda Function to update configurations dynamically
    new LambdaConstruct(this, 'LambdaConstruct', {
      redisEndpoint: redisConstruct.getRedisEndpoint(),
      snsTopicArn: snsConstruct.getTopicArn(),
      redisClusterName: redisConstruct.getRedisClusterName(),
    });

    // Create S3 Bucket
    const bucket = new s3.Bucket(this, 'AppBucket', {
      bucketName: 'leader-election-app-bucket',
      versioned: true,
      removalPolicy: RemovalPolicy.DESTROY, // Automatically delete bucket on stack destroy
      autoDeleteObjects: true, // Delete objects when the bucket is destroyed
    });

  }
}