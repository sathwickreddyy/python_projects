#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import {Environment} from "aws-cdk-lib";
import {ACCOUNT, APP_NAME, AWS_REGION, REGION} from "../lib/constants/constants";
import {LeaderElectionWithRedisStack} from "../lib/leader_election_redis_stack";
import {VpcStack} from "../lib/vpc_stack";

const app = new cdk.App();

const env: Environment = {
    account: ACCOUNT,
    region: AWS_REGION
}

const stackPrefix = `${APP_NAME}-${REGION}-`

// Create a VPC Stack
const vpcStack = new VpcStack(app, stackPrefix+'VpcStack', {
    env: env,
    stackName: stackPrefix+'VpcStack'
});

const leaderElectionWithRedisStack = new LeaderElectionWithRedisStack(app, stackPrefix+'LeaderElectionWithRedisStack', {
    env: env,
    stackName: stackPrefix+'LeaderElectionWithRedisStack',
    vpc: vpcStack.getVpc()
});
