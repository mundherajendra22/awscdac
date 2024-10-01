package com.citizens;

import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.Tags;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.dynamodb.TableEncryption;
import software.amazon.awscdk.services.ec2.CfnKeyPair;
import software.amazon.awscdk.services.iam.*;
import software.amazon.awscdk.services.kms.Key;
import software.amazon.awscdk.services.kms.KeySpec;
import software.amazon.awscdk.services.kms.KeyUsage;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sns.subscriptions.EmailSubscription;
import software.amazon.awscdk.services.ssm.StringParameter;
import software.constructs.Construct;

import java.io.IOException;
import java.util.List;

/**
 * CDK Stack that creates the pre-requisites needed for the EMR Stack
 * Creates encrypted SNS Topic, S3 Bucket for EMR logs and a VPC - that is used only for demo, local accounts.
 * You can create any infra stack that is needed for EMR
 * <p>
 * Author: Jay Mehta
 */


public class Prerequisites extends Stack {
    private final Configuration conf;
    private final String environment;
    private final String stackName;

    public Prerequisites(final Construct parent, final String id) throws IOException {
        this(parent, id, null);
    }

    /**
     * Constructor that creates initial setup for ScheduledEMRStack
     *
     * @param parent
     * @param id
     * @param props
     * @throws IOException
     */
    public Prerequisites(final Construct parent, final String id, final StackProps props) throws IOException {
        super(parent, id, props);
        this.environment = (String) getNode().tryGetContext("environment");
        this.conf = new Configuration(environment);
        this.stackName = id;

        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/s3/Bucket.html
        // Create S3 bucket to store logs from EMR Clusters
        Bucket logsBucket = Bucket.Builder.create(this, conf.getEntitiesParamValue()).bucketName(""+conf.getEntitiesParamValue()+"-emrlogsbucket-"+ this.getAccount())
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL).encryption(BucketEncryption.KMS_MANAGED).enforceSsl(true)
                .serverAccessLogsPrefix("accessLogs")
                .build();

        Bucket auditBucket = Bucket.Builder.create(this, conf.getAudits3ParamValue()).bucketName(conf.getAudits3ParamValue())
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL).encryption(BucketEncryption.S3_MANAGED).enforceSsl(true)
                .serverAccessLogsPrefix("accessLogs").build();

        Bucket dbBackUpBucket = Bucket.Builder.create(this, conf.getDBBackupParamValue()).bucketName(conf.getDBBackupParamValue())
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL).encryption(BucketEncryption.S3_MANAGED).enforceSsl(true)
                .serverAccessLogsPrefix("accessLogs").build();

        Bucket sourceBucket = Bucket.Builder.create(this, conf.getLambdaBucket()).bucketName(conf.getLambdaBucket())
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL).encryption(BucketEncryption.S3_MANAGED).enforceSsl(true)
                .serverAccessLogsPrefix("accessLogs").build();


        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/dynamodb/Table.html
        //creating dynamoDB table used for Audit&Controls
        Attribute name = Attribute.builder().name("name").type(AttributeType.STRING).build();
        Table dynamoTable = Table.Builder.create(this, "DynamoDB Table").tableName(conf.getEntitiesParamValue())
                .partitionKey(name).readCapacity(20).writeCapacity(30)
                .encryption(TableEncryption.AWS_MANAGED)
                .pointInTimeRecovery(true)
                .removalPolicy(RemovalPolicy.DESTROY)
                .build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/index.html?software/amazon/awscdk/services/ec2/CfnKeyPair.html
        //EC2 Keypair to be used by EMR
        CfnKeyPair cdacKeyPair = CfnKeyPair.Builder.create(this, conf.getEntitiesParamValue() + "-keypair")
                .keyName(conf.getEMREc2KeyName()).keyType("rsa")
                .build();


        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/ec2/Vpc.html
        // Create VPC to deploy EMR cluster. This should be used only for dev environments where there are no VPCs in
        // an account. Only used for unit testing.
        // In general, Customers will always have their network layout with VPCs, Subnets configured when deploying CDAC.
        // so this will not be needed. Hence, it has been commented out.
       /* Vpc vpc = new Vpc(this, "EMRVPC", VpcProps.builder().maxAzs(2).build());

        LogGroup logGroup = new LogGroup(this, "CDACLogGroup");
        Role role = Role.Builder.create(this, "CDACRole")
                .assumedBy(new ServicePrincipal("vpc-flow-logs.amazonaws.com"))
                .build();
        FlowLog.Builder.create(this, "FlowLog")
                .resourceType(FlowLogResourceType.fromVpc(vpc))
                .destination(FlowLogDestination.toCloudWatchLogs(logGroup, role))
                .build();*/

        //create SSM Parameters
        createSSMParameters();
        Tags.of(this).add("Team", "data-engineering");
    }

    private void createSSMParameters() {

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/ssm/StringParameter.html
        //These parameters are environment specific, so they need to be altered in the cdk.properties file of the respective environment
        //String parameter for CDA manifest key
        StringParameter manifestKey = StringParameter.Builder.create(this, "manifestKey")
                .parameterName(conf.getManifestParamName())
                .stringValue(conf.getManifestParamValue())
                .build();

        //String parameter for prefix to be used by Lambdas in CDAC
        StringParameter lambdaPrefix = StringParameter.Builder.create(this, "lambdaPrefix")
                .parameterName(conf.getLambdaPrefixParamName())
                .stringValue(conf.getLambdaPrefixParamValue())
                .build();

        StringParameter auditReportBucketName = StringParameter.Builder.create(this, "auditReportBucketName")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getAudits3ParamName())
                .stringValue(conf.getAudits3ParamValue())
                .build();

        StringParameter cdaS3BucketName = StringParameter.Builder.create(this, "cdaS3BucketName")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getCDAs3ParamName())
                .stringValue(conf.getCDAs3ParamValue())
                .build();

        StringParameter cdaS3Endpoint = StringParameter.Builder.create(this, "cdaS3Endpoint")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getCDAs3EndpointParamName())
                .stringValue(conf.getCDAs3EndpointParamValue())
                .build();

        StringParameter dbBackupBucketName = StringParameter.Builder.create(this, "dbBackupBucketName")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getDBBackupParamName())
                .stringValue(conf.getDBBackupParamValue())
                .build();

        StringParameter dropDBParam = StringParameter.Builder.create(this, "dropDBParam")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getDropDBParamName())
                .stringValue(conf.getDropDBParamValue())
                .build();

        StringParameter entities = StringParameter.Builder.create(this, "entities")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getEntitiesParamName())
                .stringValue(conf.getEntitiesParamValue())
                .build();

        StringParameter jdbcSchemaRaw = StringParameter.Builder.create(this, "jdbcSchemaRaw")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getJdbcSchemaRawParamName())
                .stringValue(conf.getJdbcSchemaRawParamValue())
                .build();

        StringParameter jdbcSchemaMerged = StringParameter.Builder.create(this, "jdbcSchemaMerged")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getJdbcSchemaMergedParamName())
                .stringValue(conf.getJdbcSchemaMergedParamValue())
                .build();

        StringParameter jdbcUrl = StringParameter.Builder.create(this, "jdbcUrl")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getJdbcUrlParamName())
                .stringValue(conf.getJdbcUrlParamValue())
                .build();

        StringParameter storedProcSchema = StringParameter.Builder.create(this, "storedProcSchemaPublic")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getStoredProcSchemaParamName())
                .stringValue(conf.getStoredProcSchemaParamValue())
                .build();

        StringParameter storedProcMergedView = StringParameter.Builder.create(this, "storedProcMergedView")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getStoredProcMergedViewParamName())
                .stringValue(conf.getStoredProcMergedViewParamValue())
                .build();

        StringParameter storedProcSQLBucket = StringParameter.Builder.create(this, "storedProcSQLBucket")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getStoredProcSQLBucketParamName())
                .stringValue(conf.getStoredProcSQLBucketParamValue())
                .build();

        StringParameter storedProcSQLS3Key = StringParameter.Builder.create(this, "storedProcSQLS3Key")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getStoredProcSQLS3KeyPathParamName())
                .stringValue(conf.getStoredProcSQLS3KeyParamValue())
                .build();

        StringParameter excludeTables = StringParameter.Builder.create(this, "excludeTables")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getExcludeTableParamName())
                .stringValue(conf.getExcludeTableParamValue())
                .build();

        StringParameter cdaS3SecretName = StringParameter.Builder.create(this, "cdaS3SecretName")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getCDAS3SecretParamName())
                .stringValue(conf.getCDAS3SecretParamValue() + "-" + conf.getEntitiesParamValue())
                .build();

        StringParameter rdsSecretName = StringParameter.Builder.create(this, "rdsSecretName")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getRDSSecretParamName())
                .stringValue(conf.getRDSSecretParamValue())
                .build();
    }
}
