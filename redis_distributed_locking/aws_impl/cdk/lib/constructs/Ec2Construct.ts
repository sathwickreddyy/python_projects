import {Construct} from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as autoscaling from 'aws-cdk-lib/aws-autoscaling';
import * as elb from 'aws-cdk-lib/aws-elasticloadbalancingv2';
import {CfnOutput, Duration} from "aws-cdk-lib";
import * as iam from 'aws-cdk-lib/aws-iam';
import {KeyPair} from "aws-cdk-lib/aws-ec2";


interface Ec2ConstructProps {
    vpc: ec2.Vpc;
}

export class Ec2Construct extends Construct {
    constructor(scope: Construct, id: string, props: Ec2ConstructProps) {
        super(scope, id);

        // Security Group for EC2 instances
        const securityGroup = new ec2.SecurityGroup(this, 'Ec2SecurityGroup', {
            securityGroupName: "leader-election-ec2-sg",
            vpc: props.vpc,
            allowAllOutbound: true,
        });

        // IAM Role for EC2 instances
        const ec2Role = new iam.Role(this, 'Ec2InstanceRole', {
          assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
          description: 'Role for EC2 instances to access S3, SNS, and other AWS services',
        });

        // Attach policies to the role
        ec2Role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3ReadOnlyAccess')); // Read access to S3
        ec2Role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore')); // Access to Systems Manager
        ec2Role.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSNSFullAccess')); // Full access to SNS

        // User Data to install dependencies on EC2 instances
        const userData = ec2.UserData.forLinux();
        userData.addCommands(
            `#!/bin/bash`,
            `sudo yum update -y`,
            `sudo yum install -y python3 pip pip3`,
            `pip install boto3 redis flask`,
            `python3 -m pip install boto3 redis flask`,
            // Download application files from S3
            `aws s3 cp s3://sathwick-pipeline-s3/core/ /home/ec2-user/ --recursive`,
            // Make sure main.py and server.py are executable
            `chmod +x /home/ec2-user/main.py`,
            `chmod +x /home/ec2-user/server.py`,
            // Start server.py in the background
            `nohup python3 /home/ec2-user/server.py > /home/ec2-user/server.log 2>&1 &`
        );

        const keyPair = KeyPair.fromKeyPairName(this, 'KeyPair', 'test-sr');

        // Auto Scaling Group
        const asg = new autoscaling.AutoScalingGroup(this, 'AutoScalingGroup', {
            autoScalingGroupName: "leader-election-asg",
            vpc: props.vpc,
            instanceType: new ec2.InstanceType('t3.micro'),
            machineImage: ec2.MachineImage.latestAmazonLinux2(),
            minCapacity: 3,
            maxCapacity: 5,
            keyPair:keyPair,
            securityGroup,
            userData,
            role: ec2Role, // Attach the IAM role to the EC2 instances
        });

        // Application Load Balancer
        const alb = new elb.ApplicationLoadBalancer(this, 'ALB', {
            loadBalancerName: "leader-election-alb",
            vpc: props.vpc,
            internetFacing: true,
        });

        alb.addListener('Listener', {
            port: 80,
            defaultTargetGroups: [
                new elb.ApplicationTargetGroup(this, 'TargetGroup', {
                    vpc: props.vpc,
                    targets: [asg],
                    port: 80,
                    healthCheck: {
                        path: '/execute',
                        interval: Duration.seconds(30),
                    },
                }),
            ],
        });

    }
}
