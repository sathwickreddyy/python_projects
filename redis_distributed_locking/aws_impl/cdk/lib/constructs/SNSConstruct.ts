import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as sns from 'aws-cdk-lib/aws-sns';

export class SnsConstruct extends Construct {
  private readonly snsTopicArn: string;

  constructor(scope: Construct, id: string) {
    super(scope, id);

    // Create SNS Topic
    const topic = new sns.Topic(this, 'LeaderElectionTopic', {
      displayName: 'Leader Election Notifications',
    });

    this.snsTopicArn = topic.topicArn;
  }

  public getTopicArn(): string {
    return this.snsTopicArn;
  }
}
