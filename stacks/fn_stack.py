from aws_cdk import (
  Stack, Duration, RemovalPolicy, Aws, CfnOutput, Environment,
  aws_s3 as s3, aws_lambda as lmb, aws_iam as iam,
  aws_events as ev, aws_events_targets as tgt,
  aws_glue as glue, aws_athena as athena
)

mk = lambda ctor: (lambda scope, id, **k: ctor(scope, id, **k))
out = lambda s,k,v: CfnOutput(s,k,value=v) or v

mk_bucket, mk_role, mk_lambda = mk(s3.Bucket), mk(iam.Role), mk(lmb.Function)
mk_rule, mk_glue_db, mk_glue_crawler = mk(ev.Rule), mk(glue.CfnDatabase), mk(glue.CfnCrawler)
mk_workgroup = mk(athena.CfnWorkGroup)

def build_stack(app, name, account, region):
    s = Stack(app, name, env=Environment(account=account, region=region))

    raw = mk_bucket(s,"Raw",
        block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        encryption=s3.BucketEncryption.S3_MANAGED, enforce_ssl=True,
        versioned=True, lifecycle_rules=[s3.LifecycleRule(expiration=Duration.days(365))],
        removal_policy=RemovalPolicy.DESTROY, auto_delete_objects=True)
    res = mk_bucket(s,"AthenaRes",
        block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        encryption=s3.BucketEncryption.S3_MANAGED, enforce_ssl=True,
        removal_policy=RemovalPolicy.DESTROY, auto_delete_objects=True)

    l_role = mk_role(s,"IngestRole",
        assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")])
    raw.grant_put(l_role)

    fn = mk_lambda(s,"Ingest",
        runtime=lmb.Runtime.PYTHON_3_12, handler="ingest.handler",
        code=lmb.Code.from_asset("lambda_src"), timeout=Duration.seconds(60), role=l_role,
        environment={"BUCKET":raw.bucket_name,"PREFIX":"raw/users",
                     "API_URL":"https://jsonplaceholder.typicode.com/users"})

    mk_rule(s,"Daily", schedule=ev.Schedule.cron(minute="0", hour="0"),
            targets=[tgt.LambdaFunction(fn)])

    db = mk_glue_db(s,"UsersDb", catalog_id=Aws.ACCOUNT_ID,
        database_input=glue.CfnDatabase.DatabaseInputProperty(name="users_db"))

    c_role = mk_role(s, "CrawlerRole",
        assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        managed_policies=[
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
        ]
    )

    # Inline policy to ensure Glue can write crawler logs to CloudWatch
    c_role.add_to_policy(iam.PolicyStatement(
        actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"],
        resources=[f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws-glue/*"]
    ))
    raw.grant_read(c_role)

    crawler = mk_glue_crawler(s,"UsersCrawler", role=c_role.role_arn, database_name=db.ref, table_prefix="users_",
        targets=glue.CfnCrawler.TargetsProperty(
            s3_targets=[glue.CfnCrawler.S3TargetProperty(path=f"s3://{raw.bucket_name}/raw/users/")]),
        schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(update_behavior="UPDATE_IN_DATABASE", delete_behavior="LOG"))

    a_role = mk_role(s,"AthenaQueryRole", assumed_by=iam.AccountRootPrincipal())
    res.grant_read_write(a_role)

    wg = mk_workgroup(s,"WG", name="wg-data-test",
        work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
            enforce_work_group_configuration=True,
            result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                output_location=f"s3://{res.bucket_name}/")))

    out(s,"RawBucket",raw.bucket_name); out(s,"LambdaName",fn.function_name)
    out(s,"GlueDatabase","users_db");   out(s,"GlueCrawler",crawler.ref)
    out(s,"AthenaWG",wg.name);          out(s,"AthenaQueryRoleArn",a_role.role_arn)
    return s