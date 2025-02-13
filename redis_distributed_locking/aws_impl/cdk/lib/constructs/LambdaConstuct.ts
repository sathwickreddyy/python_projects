import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import {ManagedPolicy, PolicyStatement, Role, ServicePrincipal} from "aws-cdk-lib/aws-iam";

interface LambdaProps {
  redisEndpoint: string;
  redisClusterName: string;
  snsTopicArn: string;
}

export class LambdaConstruct extends Construct {
  constructor(scope: Construct, id: string, props: LambdaProps) {
    super(scope, id);
    
    // Create an IAM Role for Lambda
    const lambdaRole = new Role(this, 'LambdaExecutionRole', {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
    });

    // Attach basic execution role
    lambdaRole.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'));

    // Grant access to all parameters under /app/config/
    const parameterPath = '/app/config/';
    const parameterArn = `arn:aws:ssm:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:parameter${parameterPath}`;

    lambdaRole.addToPolicy(
      new PolicyStatement({
        actions: ['ssm:GetParametersByPath'],
        resources: [parameterArn],
      })
    );

    // Add SNS publish permissions
      lambdaRole.addToPolicy(
        new PolicyStatement({
          actions: ['sns:*'],
          resources: [props.snsTopicArn],
        })
      );

    // Add ElastiCache connect permissions (if needed)
    lambdaRole.addToPolicy(
    new PolicyStatement({
      actions: ['elasticache:Connect', 'elasticache:DescribeCacheClusters'],
      resources: [`arn:aws:elasticache:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:cluster/${props.redisClusterName}`],
    })
  );


    // Define the Lambda function
    const redisLambda = new lambda.Function(this, 'RedisLambdaFunction', {
      functionName: 'RedisElectionStrategyLambdaFunction',
      runtime: lambda.Runtime.PYTHON_3_9, // Specify Python runtime
      handler: 'redis_lambda_function.handler', // File name and function name
      code: lambda.Code.fromAsset('lib/lambda'), // Path to the directory containing redis_lambda_function.py
      environment: {
        REDIS_ENDPOINT: props.redisEndpoint,
        SNS_TOPIC_ARN: props.snsTopicArn
      },
      role: lambdaRole,
    });
  }
}
