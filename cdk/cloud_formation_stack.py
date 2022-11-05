from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_rds as rds,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_notifications as s3_notify,
    aws_events as events,
    aws_events_targets as targets,
    Duration
)
from constructs import Construct

class CdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Define Lambda layers
        pandas = lambda_.LayerVersion.from_layer_version_attributes(self, 'Pandas',
            layer_version_arn="arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p39-pandas:4")

        psycopg = lambda_.LayerVersion.from_layer_version_attributes(self, 'Psycopg',
            layer_version_arn="arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p39-psycopg2-binary:1")
        
        # instantiate DB credentials using secrets manager
        db_secrets = rds.DatabaseSecret(self, 'postgres-secret',
                    username='postgres',
                    secret_name='postgres-credentials'
                    )

        # Define the function's execution role
        lambda_role = iam.Role(self, "lambda_role",
                    assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                    )
        lambda_role.add_to_policy(iam.PolicyStatement(
                    resources=["*"],
                    actions=[
                            "s3:GetObject",
                            "s3:PutObject",
                            "secretsmanager:GetSecretValue"]
                    ))
        # Create the bucket used to store the data
        s3_bucket = s3.Bucket(self, 'monks_dataBucket')

        # Create the loaf function
        load_function = lambda_.Function(self, "load_function",
                    runtime=lambda_.Runtime.PYTHON_3_9,
                    code=lambda_.Code.from_asset("./code/load_new_file"),
                    handler="LambdaListener.handler",
                    layers=[pandas, psycopg],
                    timeout=Duration.minutes(5),
                    role=lambda_role,
                    memory_size=512,
                    environment={
                        'S3_BUCKET':s3_bucket.bucket_name,
                    }
                    )

        # Create trigger for Lambda function using suffix
        notification = s3_notify.LambdaDestination(load_function)
        notification.bind(self, s3_bucket)

        # Add Create Event only for .json files
        s3_bucket.add_object_created_notification(
            notification, s3.NotificationKeyFilter(suffix='.json'))

        # Create the daily job function
        daily_function = lambda_.Function(self, "daily_function",
                    runtime=lambda_.Runtime.PYTHON_3_9,
                    code=lambda_.Code.from_asset("./code/daily_agg_job"),
                    handler="lambda_function.lambda_handler",
                    layers=[pandas, psycopg],
                    timeout=Duration.minutes(5),
                    role=lambda_role,
                    memory_size=512,
                    environment={
                        'S3_BUCKET':s3_bucket.bucket_name
                    }
                    )

        # Create the event rule and schedule
        daily_rule = events.Rule(self, "Rule",
                    schedule=events.Schedule.expression('cron(0 12 * * ? *)'),
                    )        
        daily_rule.add_target(targets.LambdaFunction(daily_function))

        # Create the database
        db = rds.DatabaseInstance(self, "db",
                    engine=rds.DatabaseInstanceEngine.postgres(version=rds.PostgresEngineVersion.VER_13_4),
                    instance_type=ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.MICRO),
                    credentials=rds.Credentials.from_secret(db_secrets),
                    vpc=ec2.Vpc(self, 'vpc'),
                    vpc_subnets=ec2.SubnetSelection(
                        subnet_type=ec2.SubnetType.PUBLIC
                    )
                    )
        
        # Allow public connection to the db
        db.connections.allow_default_port_from_any_ipv4()
