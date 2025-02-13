import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as elasticache from 'aws-cdk-lib/aws-elasticache';

interface RedisConstructProps {
  vpc: ec2.Vpc;
}

export class RedisConstruct extends Construct {
  private readonly redisEndpoint: string;
  private readonly redisClusterName: string;

  constructor(scope: Construct, id: string, props: RedisConstructProps) {
    super(scope, id);

    // Subnet group for ElastiCache
    const subnetGroup = new elasticache.CfnSubnetGroup(this, 'RedisSubnetGroup', {
      description: 'Subnet group for Redis',
      subnetIds: props.vpc.privateSubnets.map((subnet) => subnet.subnetId),
    });

    // ElastiCache Cluster
    const cluster = new elasticache.CfnCacheCluster(this, 'RedisCluster', {
      cacheNodeType: 'cache.t3.micro',
      engine: 'redis',
      numCacheNodes: 1,
      clusterName: 'leader-election-redis',
      cacheSubnetGroupName: subnetGroup.ref,
      vpcSecurityGroupIds: [
        props.vpc.vpcDefaultSecurityGroup,
      ],
    });

    this.redisEndpoint = cluster.attrRedisEndpointAddress;
    this.redisClusterName = cluster.ref;
  }

  public getRedisEndpoint(): string {
    return this.redisEndpoint;
  }

  public getRedisClusterName(): string {
    return this.redisClusterName;
  }
}
