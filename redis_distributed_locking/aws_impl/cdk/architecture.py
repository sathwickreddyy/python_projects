from diagrams import Diagram, Cluster, Edge
from diagrams.aws.network import ALB
from diagrams.aws.compute import EC2, AutoScaling
from diagrams.aws.database import ElasticacheForRedis
from diagrams.aws.management import SystemsManagerParameterStore
from diagrams.aws.integration import SimpleNotificationServiceSns  # Corrected import for SNS Topic
from diagrams.aws.network import VPC, PublicSubnet, PrivateSubnet
from diagrams.aws.compute import Lambda
from diagrams.onprem.client import User

NUMBER_OF_NODES = 3

# Create the diagram
with Diagram("Leader Election Architecture", show=True, direction="TB"):
    # User outside the VPC
    user = User("User")

    # Lambda function outside the VPC
    lambda_function = Lambda("Lambda Function")


    # VPC Cluster
    with Cluster("VPC"):
        # Subnet 1: Public Subnet with ALB
        with Cluster("Public Subnet 1"):
            alb = ALB("Application Load Balancer")

        # Subnet 2: Private Subnet with EC2 Instances
        with Cluster("Private Subnet 2"):
            ec2_asg = AutoScaling("Auto Scaling Group")
            ec2_instances = [EC2(f"EC2 Instance {i+1}") for i in range(NUMBER_OF_NODES)]

        # Subnet 3: Private Subnet with Redis
        with Cluster("Private Subnet 3"):
            redis = ElasticacheForRedis("Redis")

    # External Resources
    ssm = SystemsManagerParameterStore("SSM Parameter Store")
    sns = SimpleNotificationServiceSns("SNS Topic")  # Corrected SNS Topic representation

    # Connections
    alb >> Edge(label="Routes Request") >> ec2_asg
    for i in range(NUMBER_OF_NODES):
        if i == NUMBER_OF_NODES//2:
            ec2_instances[i] >> Edge(label="Leader election & Locks") >> redis
            redis >> Edge(label = "Sends HeartBeat of Leader to Instances", style="dotted", color="red", forward=True, reverse=True) >> ec2_instances[i]
        else:
            ec2_instances[i] >> Edge() >> redis
            redis >> Edge( style="dotted", color="red", forward=True, reverse=True) >> ec2_instances[i]

    user >> Edge(label="Hits ALB") >> lambda_function
    lambda_function >> Edge(label="Hits ALB") >> alb
    lambda_function >> Edge(label="Updates Configurations") >> ssm

    sns >> Edge(label="Websocket") >> alb
    ec2_asg >> Edge(label="Fetch Config") >> ssm
