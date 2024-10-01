package com.citizens;

import io.github.cdklabs.cdknag.NagPackSuppression;
import io.github.cdklabs.cdknag.NagSuppressions;
import software.amazon.awscdk.SecretValue;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.Tags;
import software.amazon.awscdk.services.iam.*;
import software.amazon.awscdk.services.kms.*;
import software.amazon.awscdk.services.secretsmanager.Secret;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sns.subscriptions.EmailSubscription;
import software.amazon.awscdk.services.ssm.StringParameter;
import software.amazon.awscdk.services.ssm.StringParameterAttributes;
import software.constructs.Construct;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IAMConfig extends Stack {
    private final Configuration conf;
    private final String environment;
    private final String stackName;

    public IAMConfig(final Construct parent, final String id) throws IOException {
        this(parent, id, null);
    }

    public IAMConfig(final Construct parent, final String id, final StackProps props) throws IOException {
        super(parent, id, props);
        this.environment = (String) getNode().tryGetContext("environment");
        this.conf = new Configuration(environment);
        this.stackName = id;

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/PolicyStatement.html
        //creating policy statement for log group - multiple log groups will be created for all lambdas
        PolicyStatement auditStatement1 = PolicyStatement.Builder.create()
                .sid("AuditStatement1")
                .effect(Effect.ALLOW)
                .actions(List.of("logs:CreateLogStream",
                        "logs:CreateLogGroup"))
                .resources(List.of("arn:aws:logs:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":log-group:*"))
                .build();

        //creating policy statement for s3 and log stream
        PolicyStatement auditStatement2 = PolicyStatement.Builder.create()
                .sid("AuditStatement2")
                .effect(Effect.ALLOW)
                .actions(List.of("s3:PutObject",
                        "s3:GetObject",
                        "s3:ListBucket",
                        "logs:PutLogEvents"))
                .resources(List.of("arn:aws:logs:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":log-group:*:log-stream:*",
                        "arn:aws:s3:::"+conf.getDBBackupParamValue()+"",
                        "arn:aws:s3:::"+conf.getAudits3ParamValue()+""))
                .build();

        //https://constructs.dev/packages/cdk-nag/v/2.18.42/api/NagPackSuppression?lang=java
        //Adding suppression for LogGroup and LogStreams
        NagPackSuppression nagPack1 =  NagPackSuppression.builder()
                .reason("Creating LogGroups for individual lambdas and then the events for each of them specific to this account.")
                .id("AwsSolutions-IAM5")
                .build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/PolicyDocument.html
        //Creating policy document for policy statements for Audit
        PolicyDocument auditDocument = PolicyDocument.Builder.create().statements(List.of(auditStatement1, auditStatement2)).build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/ManagedPolicy.html
        //creating managed policy for audit & dbbackup s3 bucket.
        ManagedPolicy auditReport = ManagedPolicy.Builder.create(this, "auditReport")
                .managedPolicyName("cdac-"+conf.getEntitiesParamValue()+"-dbbackup-auditreport")
                .description("Policy for creating Db Backup & Audit Report")
                .document(auditDocument)
                .build();

        //https://constructs.dev/packages/cdk-nag/v/2.18.42/api/NagSuppressions?lang=java#addResourceSuppressions
        //Suppressing policy document for loggroup and logstream creation since it's for all lambdas
        NagSuppressions.addResourceSuppressions(auditReport, List.of(nagPack1));

        //Policy statement for DynamoDB
        PolicyStatement dynamoDB1 = PolicyStatement.Builder.create()
                .sid("DynamoDB1")
                .effect(Effect.ALLOW)
                .actions(List.of("dynamodb:CreateTable",
                        "dynamodb:BatchGetItem",
                        "dynamodb:BatchWriteItem",
                        "dynamodb:PutItem",
                        "dynamodb:DeleteItem",
                        "dynamodb:GetItem",
                        "dynamodb:Scan",
                        "dynamodb:Query",
                        "dynamodb:UpdateItem",
                        "dynamodb:DeleteTable",
                        "dynamodb:UpdateTable"))
                .resources(List.of("arn:aws:dynamodb:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":table/"+conf.getEntitiesParamValue()+"",
                                   "arn:aws:dynamodb:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":table/"+conf.getEntitiesParamValue()+"-changes"))
                .build();

        //Policy statement for DynamoDB
        PolicyStatement dynamoDB2 = PolicyStatement.Builder.create()
                .sid("DynamoDB2")
                .effect(Effect.ALLOW)
                .actions(List.of("dynamodb:Scan",
                        "dynamodb:Query"))
                .resources(List.of("arn:aws:dynamodb:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":table/"+conf.getEntitiesParamValue()+"/index/*"))
                .build();

        //Policy statement for DynamoDB
        PolicyStatement dynamoDB3 = PolicyStatement.Builder.create()
                .sid("DynamoDB3")
                .effect(Effect.ALLOW)
                .actions(List.of("dynamodb:ListTables"))
                .resources(List.of("*"))
                .build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/PolicyDocument.html
        //Creating policy document for dynamoDB policy statements
        PolicyDocument dynamoDocument = PolicyDocument.Builder.create().statements(List.of(dynamoDB1, dynamoDB2, dynamoDB3)).build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/ManagedPolicy.html
        //creating managed policy for dynamoDB.
        ManagedPolicy dynamoPolicy = ManagedPolicy.Builder.create(this, "dynamoDB")
                .managedPolicyName("cdac-"+conf.getEntitiesParamValue()+"-dynamoDB")
                .description("Policy for creating DynamoDB")
                .document(dynamoDocument)
                .build();

        //https://constructs.dev/packages/cdk-nag/v/2.18.42/api/NagSuppressions?lang=java#addResourceSuppressions
        //Suppressing policy document for loggroup and logstream creation since it's for all lambdas
        NagSuppressions.addResourceSuppressions(dynamoPolicy, List.of(nagPack1));

        //Policy statement for Lambdas in a VPC
        PolicyStatement lambdaVPC1 = PolicyStatement.Builder.create()
                .sid("LambdaVPC1")
                .effect(Effect.ALLOW)
                .actions(List.of( "ec2:CreateNetworkInterface",
                        "secretsmanager:GetSecretValue",
                        "ec2:DeleteNetworkInterface",
                        "ec2:AttachNetworkInterface"))
                .resources(List.of(
                        "arn:aws:secretsmanager:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":secret:*",
                        "arn:aws:ec2:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":instance/*",
                        "arn:aws:ec2:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":network-interface/*",
                        "arn:aws:ec2:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":security-group/*",
                        "arn:aws:ec2:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":subnet/*"))
                .build();

        PolicyStatement lambdaVPC2 = PolicyStatement.Builder.create()
                .sid("LambdaVPC2")
                .effect(Effect.ALLOW)
                .actions(List.of("ec2:CreateNetworkInterface",
                        "ec2:DeleteNetworkInterface",
                        "ec2:AttachNetworkInterface"))
                .resources(List.of("arn:aws:iam::aws:policy/AWSLambdaExecute"))
                .build();

        PolicyStatement lambdaVPC3 = PolicyStatement.Builder.create()
                .sid("LambdaVPC3")
                .effect(Effect.ALLOW)
                .actions(List.of("ssm:GetParametersByPath",
                        "ssm:GetParameters",
                        "ssm:GetParameter"))
                .resources(List.of("arn:aws:ssm:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":parameter/*"))
                .build();

        PolicyStatement lambdaVPC4 = PolicyStatement.Builder.create()
                .sid("LambdaVPC4")
                .effect(Effect.ALLOW)
                .actions(List.of( "ec2:DescribeInstances",
                        "ec2:DescribeNetworkInterfaces"))
                .resources(List.of(
                        "arn:aws:iam::aws:policy/AWSLambdaExecute",
                        "arn:aws:ec2:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":instance/*",
                        "arn:aws:ec2:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":network-interface/*",
                        "arn:aws:ec2:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":security-group/*",
                        "arn:aws:ec2:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":subnet/*"
                ))
                .build();

        PolicyStatement lambdaVPC5 = PolicyStatement.Builder.create()
                .sid("LambdaVPC5")
                .effect(Effect.ALLOW)
                .actions(List.of("ssm:DescribeParameters"))
                .resources(List.of(
                        "arn:aws:ec2:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":instance/*",
                        "arn:aws:ec2:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":network-interface/*",
                        "arn:aws:ec2:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":security-group/*",
                        "arn:aws:ec2:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":subnet/*"
                        ))
                .build();

        PolicyStatement lambdaVPC6 = PolicyStatement.Builder.create()
                .sid("LambdaVPC6")
                .effect(Effect.ALLOW)
                .actions(List.of("s3:ListBucketMultipartUploads",
                        "s3:ListBucket",
                        "s3:GetBucketLocation",
                        "s3:GetObjectVersionAcl",
                        "s3:GetObjectVersion",
                        "s3:GetObjectAcl",
                        "s3:GetObject",
                        "s3:AbortMultipartUpload"))
                .resources(List.of(
                        "arn:aws:s3:::"+conf.getCDAs3ParamValue()+"",
                        "arn:aws:s3:::"+conf.getCDAs3ParamValue()+"/*"
                ))
                .build();

        //Get audit report bucket name from SSM
        String auditReportBucketName = StringParameter.fromStringParameterAttributes(this, "auditReportBucketName", StringParameterAttributes.builder()
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getAudits3ParamName())
                .build()).getStringValue();

        String reconLambdaBucketName = StringParameter.fromStringParameterAttributes(this, "reconLambdaBucketName", StringParameterAttributes.builder()
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getReconLambdaS3ParamName())
                .build()).getStringValue();

        PolicyStatement lambdaVPC7 = PolicyStatement.Builder.create()
                .sid("LambdaVPC7")
                .effect(Effect.ALLOW)
                .actions(List.of(
                        "s3:List*",
                        "s3:Put*",
                        "s3:Get*"))
                .resources(List.of(
                        "arn:aws:s3:::"+auditReportBucketName+"",
                        "arn:aws:s3:::"+auditReportBucketName+"/*"
                ))
                .build();

        PolicyStatement lambdaVPC8 = PolicyStatement.Builder.create()
                .sid("LambdaVPC8")
                .effect(Effect.ALLOW)
                .actions(List.of(
                        "s3:List*",
                        "s3:Put*",
                        "s3:Get*"))
                .resources(List.of(
                        "arn:aws:s3:::"+reconLambdaBucketName+"",
                        "arn:aws:s3:::"+reconLambdaBucketName+"/*"
                ))
                .build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/PolicyDocument.html
        //Creating policy document for lambdaVPC policy statements
        PolicyDocument lambdaVPCDocument = PolicyDocument.Builder.create().statements(List.of(lambdaVPC1, lambdaVPC2, lambdaVPC3, lambdaVPC4, lambdaVPC5, lambdaVPC6, lambdaVPC7, lambdaVPC8)).build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/ManagedPolicy.html
        //creating managed policy for lambda VPC.
        ManagedPolicy lambdaVPCPolicy = ManagedPolicy.Builder.create(this, "lambdaVPC")
                .managedPolicyName("cdac-"+conf.getEntitiesParamValue()+"-lambdaVPC")
                .description("Policy for LambdaVPC")
                .document(lambdaVPCDocument)
                .build();

        //https://constructs.dev/packages/cdk-nag/v/2.18.42/api/NagSuppressions?lang=java#addResourceSuppressions
        //Suppressing policy document for LambdaVPC policy creation since it's for all lambdas
        NagSuppressions.addResourceSuppressions(lambdaVPCPolicy, List.of(nagPack1));

        //Policy statement for RDS s3 integration
        PolicyStatement rdsS3Int1 = PolicyStatement.Builder.create()
                .sid("RDSS3Int1")
                .effect(Effect.ALLOW)
                .actions(List.of("kms:Decrypt",
                        "kms:GenerateDataKey",
                        "kms:DescribeKey",
                        "s3:ListBucket",
                        "s3:GetBucketLocation"))
                .resources(List.of("arn:aws:s3:::"+conf.getDBBackupParamValue()+"",
                        "arn:aws:kms:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":key/alias/aws/rds"))
                .build();

        PolicyStatement rdsS3Int2 = PolicyStatement.Builder.create()
                .sid("RDSS3Int2")
                .effect(Effect.ALLOW)
                .actions(List.of("s3:PutObject",
                        "s3:GetObject",
                        "s3:AbortMultipartUpload",
                        "s3:ListMultipartUploadParts"))
                .resources(List.of("arn:aws:s3:::"+conf.getDBBackupParamValue()+"/"+conf.getEntitiesParamValue()+""))
                .build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/PolicyDocument.html
        //Creating policy document for RDS s3 integration policy statements
        PolicyDocument rdsS3Int = PolicyDocument.Builder.create().statements(List.of(rdsS3Int1, rdsS3Int2)).build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/ManagedPolicy.html
        //creating managed policy for RDS S3 Integration.
        ManagedPolicy rdsS3Policy = ManagedPolicy.Builder.create(this, "rdsS3Integration")
                .managedPolicyName("cdac-"+conf.getEntitiesParamValue()+"-rdss3Int")
                .description("Policy for RDS S3 Integration")
                .document(rdsS3Int)
                .build();

        //https://constructs.dev/packages/cdk-nag/v/2.18.42/api/NagSuppressions?lang=java#addResourceSuppressions
        //Suppressing policy document for RDS S3 Integration policy creation since it will be used for all environment folders
        NagSuppressions.addResourceSuppressions(rdsS3Policy, List.of(nagPack1));

        //Policy Statement for RDS Stop/Start
        PolicyStatement rdsStopStart1 = PolicyStatement.Builder.create()
                .sid("RDSStopStart1")
                .effect(Effect.ALLOW)
                .actions(List.of("rds:StartDBCluster",
                        "rds:StopDBCluster",
                        "rds:DescribeDBSnapshots",
                        "rds:CreateDBSnapshot",
                        "rds:DescribeDBClusterSnapshots",
                        "rds:DescribeDBInstances",
                        "rds:CreateDBClusterSnapshot",
                        "rds:StopDBInstance",
                        "rds:DescribeDBClusters",
                        "rds:StartDBInstance"))
                .resources(List.of("arn:aws:rds:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":cluster:*",
                        "arn:aws:rds:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":snapshot:*",
                        "arn:aws:rds:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":cluster-snapshot:*",
                        "arn:aws:rds:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":db:*"))
                .build();

        PolicyStatement rdsStopStart2 = PolicyStatement.Builder.create()
                .sid("RDSStopStart2")
                .effect(Effect.ALLOW)
                .actions(List.of("rds:DescribeExportTasks",
                        "rds:StartExportTask"))
                .resources(List.of("*"))
                .build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/PolicyDocument.html
        //Creating policy document for RDS Stop start policy statements
        PolicyDocument rdsStopStart = PolicyDocument.Builder.create().statements(List.of(rdsStopStart1, rdsStopStart2)).build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/ManagedPolicy.html
        //creating managed policy for RDS Stop start.
        ManagedPolicy rdsStopStartPolicy = ManagedPolicy.Builder.create(this, "rdsStopStart")
                .managedPolicyName("cdac-"+conf.getEntitiesParamValue()+"-rdsStopStart")
                .description("Policy for RDS Stop Start")
                .document(rdsStopStart)
                .build();

        //https://constructs.dev/packages/cdk-nag/v/2.18.42/api/NagSuppressions?lang=java#addResourceSuppressions
        //Suppressing policy document for RDS Stop Start policy
        NagSuppressions.addResourceSuppressions(rdsStopStartPolicy, List.of(nagPack1));

        //Policy Statement for Step function
        Map<String, String[]> subCondition1 = new HashMap<String, String[]>();
        subCondition1.put("iam:AWSServiceName", new String[]{"elasticmapreduce.amazonaws.com"} );
        Map<String, Map<String, String[]>> conditions1 = new HashMap<String, Map<String, String[]>>();
        conditions1.put("StringLike",subCondition1);
        PolicyStatement stepFunctionStatement1 = PolicyStatement.Builder.create()
                .sid("StepFunction1")
                .effect(Effect.ALLOW)
                .actions(List.of("iam:CreateServiceLinkedRole"))
                .resources(List.of("arn:aws:iam::*:role/aws-service-role/elasticmapreduce.amazonaws.com*/AWSServiceRoleForEMRCleanup*"))
                .conditions(conditions1)
                .build();

        PolicyStatement stepFunctionStatement2 = PolicyStatement.Builder.create()
                .sid("StepFunction2")
                .effect(Effect.ALLOW)
                .actions(List.of("events:PutTargets",
                        "iam:PassRole",
                        "events:DescribeRule",
                        "events:PutRule",
                        "elasticmapreduce:DescribeCluster",
                        "elasticmapreduce:TerminateJobFlows"))
                .resources(List.of( "arn:aws:iam::"+conf.getCustomerAccountId()+":role/*",
                        "arn:aws:events:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":rule/StepFunctionsGetEventForEMRRunJobFlowRule",
                        "arn:aws:elasticmapreduce:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":cluster/*"))
                .build();

        PolicyStatement stepFunctionStatement3 = PolicyStatement.Builder.create()
                .sid("StepFunction3")
                .effect(Effect.ALLOW)
                .actions(List.of("events:PutTargets",
                        "events:DescribeRule",
                        "iam:PassRole",
                        "events:PutRule"))
                .resources(List.of( "arn:aws:events:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":rule/StepFunctionsGetEventForEMRAddJobFlowStepsRule",
                        "arn:aws:iam::"+conf.getCustomerAccountId()+":role/EMR_AutoScaling_DefaultRole"))
                .build();

        PolicyStatement stepFunctionStatement4 = PolicyStatement.Builder.create()
                .sid("StepFunction4")
                .effect(Effect.ALLOW)
                .actions(List.of("rds:AddTagsToResource",
                        "events:DescribeRule",
                        "rds:DescribeDBSnapshots",
                        "lambda:InvokeFunction",
                        "events:PutRule",
                        "iam:PutRolePolicy",
                        "elasticmapreduce:DescribeCluster",
                        "events:PutTargets",
                        "elasticmapreduce:DescribeStep",
                        "secretsmanager:GetSecretValue",
                        "sns:Publish",
                        "rds:CreateDBSnapshot",
                        "kms:Create*", "kms:Describe*", "kms:Enable*", "kms:List*", "kms:Put*", "kms:GenerateDataKey",
                        "elasticmapreduce:AddTags",
                        "elasticmapreduce:AddJobFlowSteps",
                        "elasticmapreduce:CancelSteps"))
                .resources(List.of(  "arn:aws:events:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":rule/StepFunctionsGetEventForEMRTerminateJobFlowsRule",
                        "arn:aws:secretsmanager:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":secret:*",
                        "arn:aws:rds:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":snapshot:*",
                        "arn:aws:rds:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":db:*",
                        "arn:aws:lambda:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":function:"+conf.getEntitiesParamValue()+"*",
                        "arn:aws:lambda:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":function:"+conf.getEntitiesParamValue()+"*:*",
                        "arn:aws:elasticmapreduce:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":cluster/*",
                        "arn:aws:iam::*:role/aws-service-role/elasticmapreduce.amazonaws.com*/AWSServiceRoleForEMRCleanup*",
                        "arn:aws:sns:"+conf.getCustomerRegion()+":"+conf.getCustomerAccountId()+":"+conf.getSNSTopic()+""
                        ))
                .build();

        PolicyStatement stepFunctionStatement5 = PolicyStatement.Builder.create()
                .sid("StepFunction5")
                .effect(Effect.ALLOW)
                .actions(List.of( "xray:PutTelemetryRecords",
                        "logs:DescribeLogGroups",
                        "rds:StartExportTask",
                        "elasticmapreduce:DescribeCluster",
                        "xray:GetSamplingTargets",
                        "logs:GetLogDelivery",
                        "logs:ListLogDeliveries",
                        "xray:PutTraceSegments",
                        "logs:CreateLogDelivery",
                        "logs:PutResourcePolicy",
                        "logs:UpdateLogDelivery",
                        "xray:GetSamplingRules",
                        "logs:DeleteLogDelivery",
                        "elasticmapreduce:RunJobFlow",
                        "logs:DescribeResourcePolicies",
                        "elasticmapreduce:TerminateJobFlows",
                        "compute-optimizer:GetEnrollmentStatus",
                        "elasticmapreduce:PutManagedScalingPolicy",
                        "elasticmapreduce:PutAutoScalingPolicy"))
                .resources(List.of("*"))
                .build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/PolicyDocument.html
        //Creating policy document for Step function policy statements
        PolicyDocument stepFunctionDocument = PolicyDocument.Builder.create().statements(List.of(stepFunctionStatement1, stepFunctionStatement2, stepFunctionStatement3, stepFunctionStatement4, stepFunctionStatement5)).build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/ManagedPolicy.html
        //creating managed policy for Step function.
        ManagedPolicy stepFunctionPolicy = ManagedPolicy.Builder.create(this, "StepFunction")
                .managedPolicyName("cdac-"+conf.getEntitiesParamValue()+"-stepFunction")
                .description("Policy for StepFunction")
                .document(stepFunctionDocument)
                .build();

        //https://constructs.dev/packages/cdk-nag/v/2.18.42/api/NagSuppressions?lang=java#addResourceSuppressions
        //Suppressing policy document for StepFunction policy
        NagSuppressions.addResourceSuppressions(stepFunctionPolicy, List.of(nagPack1));

        IManagedPolicy awsLambdaPolicy = ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaRole");
        IManagedPolicy awsSSMReadOnly = ManagedPolicy.fromAwsManagedPolicyName("AmazonSSMReadOnlyAccess");
        IManagedPolicy awsVPCExec = ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaVPCAccessExecutionRole");

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/Role.html
        //Creating step function role
        Role stepFunctionRole = Role.Builder.create(this, "StepFunction Role").roleName("cdac-"+conf.getEntitiesParamValue()+"-stepfunction")
                .managedPolicies(List.of(auditReport, dynamoPolicy, rdsStopStartPolicy, stepFunctionPolicy, awsLambdaPolicy, awsSSMReadOnly))
                .assumedBy(new ServicePrincipal("states.amazonaws.com"))
                .build();

        //Creating lambda vpc audit report role
        Role lambdaVPCAuditRole = Role.Builder.create(this, "Lambda Vpc Audit Role").roleName("cdac-"+conf.getEntitiesParamValue()+"-lambdaVPCAudit")
                .managedPolicies(List.of(dynamoPolicy, lambdaVPCPolicy, awsLambdaPolicy, awsVPCExec))
                .assumedBy(new ServicePrincipal("lambda.amazonaws.com"))
                .build();

        //Creating lambda vpc role
        Role lambdaRDSRole = Role.Builder.create(this, "Lambda Vpc Role").roleName("cdac-"+conf.getEntitiesParamValue()+"-lambdaVPC")
                .managedPolicies(List.of(lambdaVPCPolicy, awsLambdaPolicy, awsVPCExec))
                .assumedBy(new ServicePrincipal("lambda.amazonaws.com"))
                .build();

        //Creating RDS S3 integration role
        Role rdsS3IntRole = Role.Builder.create(this, "RDS S3 Integration Role").roleName("cdac-"+conf.getEntitiesParamValue()+"-rds-s3-integration")
                .managedPolicies(List.of(rdsS3Policy))
                .assumedBy(new ServicePrincipal("rds.amazonaws.com"))
                .build();

        //EMR Default roles
        //Commenting out EMR role creation since they were created by EMC Cloud Admin
        /*Role emrAutoScaleRole = Role.Builder.create(this, "EMR AutoScale role").roleName(conf.getEMRAutoscalingRole())
                .managedPolicies(List.of(ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonElasticMapReduceforAutoScalingRole")))
                .assumedBy(new ServicePrincipal("elasticmapreduce.amazonaws.com"))
                .build();

        Role emrDefaultRole = Role.Builder.create(this, "EMR Default role").roleName(conf.getEMRServiceRole())
                .managedPolicies(List.of(ManagedPolicy.fromAwsManagedPolicyName("service-role/c")))
                .assumedBy(new ServicePrincipal("elasticmapreduce.amazonaws.com"))
                .build();

        Role emrEC2Role = Role.Builder.create(this, "EMR EC2 role").roleName(conf.getEMRClusterRole())
                .managedPolicies(List.of(ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonElasticMapReduceforEC2Role"), awsSSMReadOnly))
                .assumedBy(new ServicePrincipal("ec2.amazonaws.com"))
                .build();

         */

        //https://constructs.dev/packages/cdk-nag/v/2.18.42/api/NagPackSuppression?lang=java
        //Adding suppression for Lambda Role
        NagPackSuppression nagPack2 =  NagPackSuppression.builder()
                .reason("Using AWS Managed lambda role for CDAC Lambdas.")
                .id("AwsSolutions-IAM4")
                .build();

        //https://constructs.dev/packages/cdk-nag/v/2.18.42/api/NagSuppressions?lang=java#addResourceSuppressions
        //Suppressing IAM4 for Lambda role
        NagSuppressions.addResourceSuppressions(stepFunctionRole, List.of(nagPack2));
        NagSuppressions.addResourceSuppressions(lambdaVPCAuditRole, List.of(nagPack2));
        //NagSuppressions.addResourceSuppressions(emrEC2Role, List.of(nagPack2));
        NagSuppressions.addResourceSuppressions(lambdaRDSRole, List.of(nagPack2));
        //NagSuppressions.addResourceSuppressions(emrAutoScaleRole, List.of(nagPack2));
        //NagSuppressions.addResourceSuppressions(emrDefaultRole, List.of(nagPack2));


        PolicyStatement uploadToS3 = PolicyStatement.Builder.create()
                .sid("UploadToS3")
                .effect(Effect.ALLOW)
                .actions(List.of("s3:PutObject",
                        "s3:GetObject",
                        "s3:ListBucket"))
                .resources(List.of(
                        "arn:aws:s3:::"+conf.getLambdaBucket()+"",
                        "arn:aws:s3:::"+conf.getLambdaBucket()+"/*",
                        "arn:aws:s3:::cdk-hnb659fds-assets-*",
                        "arn:aws:s3:::cdk-hnb659fds-assets-*/*",
                        "arn:aws:s3:::"+conf.getLambdaBucket()+"/lambda1"))
                .build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/PolicyDocument.html
        //Creating policy document for RDS Stop start policy statements
        PolicyDocument uploadToS3document = PolicyDocument.Builder.create().statements(List.of(uploadToS3)).build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/ManagedPolicy.html
        //creating managed policy for RDS Stop start.
        ManagedPolicy uploadToS3Policy = ManagedPolicy.Builder.create(this, "uploadToS3Policy")
                .managedPolicyName("cdac-"+conf.getEntitiesParamValue()+"-uploadToS3")
                .description("Policy for S3 deployment")
                .document(uploadToS3document)
                .build();

        Role uploadToS3Role = Role.Builder.create(this, "UploadToS3Role").roleName("cdac-"+conf.getEntitiesParamValue()+"-uploadToS3")
                .description("Upload jars to s3 for CDAC")
                .managedPolicies(List.of(uploadToS3Policy, awsLambdaPolicy))
                .assumedBy(new ServicePrincipal("lambda.amazonaws.com"))
                .build();
        NagSuppressions.addResourceSuppressions(uploadToS3Policy, List.of(nagPack1));
        NagSuppressions.addResourceSuppressions(uploadToS3Role, List.of(nagPack2));

        ManagedPolicy gwCDABucketPolicy = ManagedPolicy.Builder.create(this, "CDABucket")
                .managedPolicyName("cdac-"+conf.getEntitiesParamValue()+"-cdabucket-policy")
                .description("access to gw cda s3 bucket")
                .document(PolicyDocument.Builder.create().statements(List.of(lambdaVPC6)).build())
                .build();
        IRole ec2Role = Role.fromRoleName(this, "EC2Role", conf.getEMRClusterRole());
        ec2Role.addManagedPolicy(gwCDABucketPolicy);

        NagSuppressions.addResourceSuppressions(gwCDABucketPolicy, List.of(nagPack1));

        IRole emrRole = Role.fromRoleArn(this, "EMRRoleforEC2", "arn:aws:iam::"+conf.getCustomerAccountId()+":role/"+conf.getEMRClusterRole()+"");

        PolicyStatement secretStatement = PolicyStatement.Builder.create()
                .sid("Secrets1")
                .effect(Effect.ALLOW)
                .actions(List.of("secretsmanager:GetSecretValue"))
                .resources(List.of("arn:aws:secretsmanager:*:"+conf.getCustomerAccountId()+":secret:*"))
                .build();

        PolicyStatement ssmStatement = PolicyStatement.Builder.create()
                .sid("GetSSMParams1")
                .effect(Effect.ALLOW)
                .actions(List.of(
                        "ssm:Describe*",
                        "ssm:Get*",
                        "ssm:List*"))
                .resources(List.of("*"))
                .build();

        PolicyDocument secretDocument = PolicyDocument.Builder.create().statements(List.of(secretStatement, ssmStatement)).build();

        Policy newPolicy = Policy.Builder.create(this, "SSMPolicy").policyName("SM_SSMAccess")
                .document(secretDocument)
                .build();
        emrRole.attachInlinePolicy(newPolicy);

        NagSuppressions.addResourceSuppressions(newPolicy, List.of(nagPack1));


        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/kms/Key.html
        PolicyStatement keyPolicyStatement = PolicyStatement.Builder.create()
                .sid("KMSKeyPolicy1")
                .effect(Effect.ALLOW)
                .actions(List.of("kms:Create*", "kms:Describe*", "kms:Enable*", "kms:List*", "kms:Put*", "kms:Gen*", "kms:Enc*", "kms:Dec*"))
                .principals(List.of(new AccountRootPrincipal(), new AccountPrincipal(this.getAccount()),
                        stepFunctionRole))
                .resources(List.of("*"))
                .build();

        //Key key = Key.Builder.create(this, "SNSKey").enableKeyRotation(false).alias("cdac/sns-" + conf.getEntitiesParamValue()).description("KMS key for SNS").enabled(true)
        //        .keySpec(KeySpec.SYMMETRIC_DEFAULT).keyUsage(KeyUsage.ENCRYPT_DECRYPT).enableKeyRotation(true)
        //        .policy(PolicyDocument.Builder.create().statements(List.of(keyPolicyStatement)).build())
        //        .build();

        PolicyStatement keyPolicyStatement1 = PolicyStatement.Builder.create()
                .sid("KMSKeyPolicy2")
                .effect(Effect.ALLOW)
                .actions(List.of("kms:Create*", "kms:Describe*", "kms:Enable*", "kms:List*", "kms:Put*", "kms:Gen*", "kms:Enc*", "kms:Dec*"))
                .resources(List.of("*"))
                .build();

        ManagedPolicy keyManagedPolicy = ManagedPolicy.Builder.create(this, "KMSKeyPolicy")
                .managedPolicyName("cdac-"+conf.getEntitiesParamValue()+"-kmssns")
                .description("Access to KMS key for StepFunction role")
                .document(PolicyDocument.Builder.create().statements(List.of(keyPolicyStatement1)).build())
                .build();

        stepFunctionRole.addManagedPolicy(keyManagedPolicy);

        NagSuppressions.addResourceSuppressions(keyManagedPolicy,List.of(nagPack1));

		// https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/sns/Topic.html
        // SNS Topic where all notifications from the StepFunctions State Machine will be sent
        //Topic topic = Topic.Builder.create(this, conf.getEntitiesParamValue() + "-topic")
        //        .displayName(conf.getSNSTopic())
         //       .topicName(conf.getSNSTopic())
         //       .masterKey(key).build();

        String[] emailNotifications = conf.getEmailForNotifications().split(",");
        EmailSubscription awsSubscription = null;

        //https://docs.aws.amazon.com/cdk/api/v1/java/index.html?software/amazon/awscdk/services/sns/subscriptions/EmailSubscription.html
        //comma separated email addresses subscribed to topic
        //for (String emailNotification : emailNotifications) {
        //    awsSubscription = EmailSubscription.Builder.create(emailNotification).build();
        //    topic.addSubscription(awsSubscription);
        //}

        Tags.of(this).add("Team", "data-engineering");
    }
}