import { Construct } from 'constructs';
import * as ssm from 'aws-cdk-lib/aws-ssm';

interface SsmProps {
  redisEndpoint: string;
  snsTopicArn: string;
}

export class SsmConstruct extends Construct {

  constructor(scope: Construct, id: string, props: SsmProps) {
    super(scope, id);

    // Store Redis endpoint in SSM Parameter Store
    new ssm.StringParameter(this, 'RedisEndpointParameter', {
      parameterName: '/app/config/redis_endpoint',
      stringValue: props.redisEndpoint,
    });

    // Store SNS Topic ARN in SSM Parameter Store
    new ssm.StringParameter(this, 'SnsTopicArnParameter', {
      parameterName: '/app/config/sns_topic_arn',
      stringValue: props.snsTopicArn,
    });
  }
}
