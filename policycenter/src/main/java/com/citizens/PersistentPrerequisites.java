package com.citizens;

import io.github.cdklabs.cdknag.NagPackSuppression;
import io.github.cdklabs.cdknag.NagSuppressions;
import software.amazon.awscdk.*;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.dynamodb.TableEncryption;
import software.amazon.awscdk.services.ec2.CfnKeyPair;
import software.amazon.awscdk.services.emr.CfnCluster;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.ssm.StringParameter;
import software.constructs.Construct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 CDK Stack that creates the pre-requisites needed for the EMR Stack
 Creates encrypted SNS Topic, S3 Bucket for EMR logs and a VPC - that is used only for demo, local accounts.
 You can create any infra stack that is needed for EMR

 Author: Jay Mehta
 * */


public class PersistentPrerequisites extends Stack {
    private final Configuration conf;
    private final String environment;
    private final String stackName;

    public PersistentPrerequisites(final Construct parent, final String id) throws IOException {
        this(parent, id, null);
    }

    /**
     * Constructor that creates initial setup for ScheduledEMRStack
     * @param parent
     * @param id
     * @param props
     * @throws IOException
     */
    public PersistentPrerequisites(final Construct parent, final String id, final StackProps props) throws IOException {
        super(parent, id, props);
        this.environment = (String) getNode().tryGetContext("environment");
        this.conf = new Configuration(environment);
        this.stackName = id;



        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/s3/Bucket.html
        // Create S3 bucket to store logs from EMR Clusters
        Bucket logsBucket = Bucket.Builder.create(this, conf.getEmrLogsS3ParamValue()).bucketName(conf.getEmrLogsS3ParamValue()+"-"+ this.getAccount())
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL).encryption(BucketEncryption.S3_MANAGED).enforceSsl(true)
                .serverAccessLogsPrefix("accessLogs")
                .build();

        Bucket auditBucket = Bucket.Builder.create(this, conf.getAudits3ParamValue()).bucketName(conf.getAudits3ParamValue()+"-"+ this.getAccount())
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL).encryption(BucketEncryption.S3_MANAGED).enforceSsl(true)
                .serverAccessLogsPrefix("accessLogs").build();

        Bucket dbBackUpBucket = Bucket.Builder.create(this, conf.getDBBackupParamValue()).bucketName(conf.getDBBackupParamValue()+"-"+ this.getAccount())
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL).encryption(BucketEncryption.S3_MANAGED).enforceSsl(true)
                .serverAccessLogsPrefix("accessLogs").build();

        Bucket sourceBucket = Bucket.Builder.create(this, conf.getLambdaBucket()+"-lambda").bucketName(conf.getLambdaBucket()+"-"+ this.getAccount())
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL).encryption(BucketEncryption.S3_MANAGED).enforceSsl(true)
                .serverAccessLogsPrefix("accessLogs").build();

				
        Attribute name = Attribute.builder().name("name").type(AttributeType.STRING).build();
        Table dynamoTable = Table.Builder.create(this, "DynamoDB Table").tableName(conf.getEntitiesParamValue())
                .partitionKey(name).readCapacity(20).writeCapacity(30)
                .encryption(TableEncryption.AWS_MANAGED)
                .build();

        // Create DynamoDB table to keep entities which has been changed
        Attribute stepFunctionExecutionId = Attribute.builder().name("stepFunctionExecutionId").type(AttributeType.STRING).build();
        Table dynamoTableChange = Table.Builder.create(this, "DynamoDB Change Table").tableName(conf.getEntitiesParamValue()+"-changes")
                .partitionKey(stepFunctionExecutionId)
                .sortKey(name)
                .readCapacity(20).writeCapacity(30)
                .encryption(TableEncryption.AWS_MANAGED)
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


        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/emr/CfnCluster.html
        // Create EMR Cluster.
        CfnCluster createCluster = createEMRCluster();

        //StringParameter emrClusterId = StringParameter.Builder.create(this, "emrClusterId")
         //       .parameterName(conf.getLambdaPrefixParamValue() + "emrClusterId")
         //       .stringValue(createCluster)
         //       .build();

        createSSMParameters();
		
        Tags.of(this).add("Team", "data-engineering");
    }

    private CfnCluster createEMRCluster() {
        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/emr/CfnCluster.ScalingRuleProperty.html
        // Autoscaling policy to be applied to EMR Clusters based on CloudWatch metric
        // Uses yarn memory available metric to scale out clusters
        /*CfnCluster.ScalingRuleProperty scaleOutRule = CfnCluster.ScalingRuleProperty.builder()
                .action(CfnCluster.ScalingActionProperty.builder()
                        .simpleScalingPolicyConfiguration(CfnCluster.SimpleScalingPolicyConfigurationProperty.builder()
                                .scalingAdjustment(2)
                                // the properties below are optional
                                .adjustmentType("CHANGE_IN_CAPACITY")
                                .coolDown(300)
                                .build())
                        // the properties below are optional
                        .build())
                .name("AutoScaleOutPolicy")
                .trigger(CfnCluster.ScalingTriggerProperty.builder()
                        .cloudWatchAlarmDefinition(CfnCluster.CloudWatchAlarmDefinitionProperty.builder()
                                .comparisonOperator("LESS_THAN_OR_EQUAL")
                                .metricName("YARNMemoryAvailablePercentage")
                                .period(60)
                                // the properties below are optional
                                .dimensions(Arrays.asList(CfnCluster.MetricDimensionProperty.builder()
                                        .key("JobFlowId")
                                        .value("${emr.clusterID}")
                                        .build()))
                                .evaluationPeriods(1)
                                .namespace("AWS/ElasticMapReduce")
                                .statistic(Statistic.AVERAGE.name())
                                .threshold(40)
                                .unit(Unit.PERCENT.name())
                                .build())
                        .build())
                // the properties below are optional
                .description("ScaleOut Policy")
                .build();

        CfnCluster.ScalingRuleProperty scaleInRule = CfnCluster.ScalingRuleProperty.builder()
                .action(CfnCluster.ScalingActionProperty.builder()
                        .simpleScalingPolicyConfiguration(CfnCluster.SimpleScalingPolicyConfigurationProperty.builder()
                                .scalingAdjustment(1)
                                // the properties below are optional
                                .adjustmentType("CHANGE_IN_CAPACITY")
                                .coolDown(300)
                                .build())
                        .build())
                .name("AutoScaleInPolicy")
                .trigger(CfnCluster.ScalingTriggerProperty.builder()
                        .cloudWatchAlarmDefinition(CfnCluster.CloudWatchAlarmDefinitionProperty.builder()
                                .comparisonOperator("GREATER_THAN_OR_EQUAL")
                                .metricName("YARNMemoryAvailablePercentage")
                                .period(60)
                                // the properties below are optional
                                .dimensions(Arrays.asList(CfnCluster.MetricDimensionProperty.builder()
                                        .key("JobFlowId")
                                        .value("${emr.clusterID}")
                                        .build()))
                                .evaluationPeriods(1)
                                .namespace("AWS/ElasticMapReduce")
                                .statistic(Statistic.AVERAGE.name())
                                .threshold(90)
                                .unit(Unit.PERCENT.name())
                                .build())
                        .build())
                // the properties below are optional
                .description("ScaleIn Policy")
                .build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/emr/CfnCluster.AutoScalingPolicyProperty.html
        //EMR AutoScalingPolicy where the scaleIn/scaleOut rules will be applied
        CfnCluster.AutoScalingPolicyProperty autoScalingPolicyProperty = CfnCluster.AutoScalingPolicyProperty.builder()
                .constraints(CfnCluster.ScalingConstraintsProperty.builder()
                        .maxCapacity(10)
                        .minCapacity(1)
                        .build())
                .rules(Arrays.asList(scaleOutRule, scaleInRule))
                .build();
				*/

        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/emr/CfnCluster.InstanceGroupConfigProperty.html
        // Configuration defining a new instance group.
        CfnCluster.InstanceGroupConfigProperty masterConfigGroup = CfnCluster.InstanceGroupConfigProperty
                .builder()
                .instanceCount(conf.getEMRMasterInstanceCount())
                .name("Master")
                .instanceType(conf.getEMRMasterInstanceType())
                .build();

        CfnCluster.InstanceGroupConfigProperty coreConfigGroup = CfnCluster.InstanceGroupConfigProperty
                .builder()
                .instanceCount(conf.getEMRCoreInstanceCount())
                .name("Core")
                .instanceType(conf.getEMRCoreInstanceType())
                //.autoScalingPolicy(autoScalingPolicyProperty)
                .build();

        CfnCluster.InstanceGroupConfigProperty taskConfigGroup = CfnCluster.InstanceGroupConfigProperty
                .builder()
                .instanceCount(conf.getEMRTaskInstanceCount())
                .name("Task")
                .instanceType(conf.getEMRTaskInstanceType())
                //.autoScalingPolicy(autoScalingPolicyProperty)
                .build();
        final ArrayList<CfnCluster.InstanceGroupConfigProperty> instanceGroups = new ArrayList<>();
        instanceGroups.add(taskConfigGroup);

        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/emr/CfnCluster.ApplicationProperty.html
        // Properties for the EMR Cluster Applications.
        final List<CfnCluster.ApplicationProperty> appList = new ArrayList<>();
        CfnCluster.ApplicationProperty sparkApp = CfnCluster.ApplicationProperty.builder()
                .name("Spark").build();
        appList.add(sparkApp);

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/CfnTag.html
        // Adding cfn tags for EMR Cluster
        final CfnTag emrTags = CfnTag.builder().key("Name").value(conf.getEMRName()).key("Environment").value(environment).build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/emr/CfnCluster.JobFlowInstancesConfigProperty.html
        // Configure the EMR clusters with the required config and instance groups
        CfnCluster.JobFlowInstancesConfigProperty instances = CfnCluster.JobFlowInstancesConfigProperty.builder()
                        .masterInstanceGroup(masterConfigGroup)
                                .coreInstanceGroup(coreConfigGroup)
                                        .taskInstanceGroups(instanceGroups)
                                                .ec2KeyName(conf.getEMREc2KeyName())
                                                        .ec2SubnetId(conf.getEMRSubnetId())
                .terminationProtected(Boolean.TRUE)
                .build();

        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/emr/CfnCluster.html
        // Creating an EMR Cluster.
        CfnCluster emrCluster = CfnCluster.Builder.create(this, "Create_an_EMR_Cluster")
                .name(conf.getEMRName())
                .applications(appList)
                .releaseLabel(conf.getEMRReleaseLabel())
                .instances(instances)
                .serviceRole(conf.getEMRServiceRole())
                .autoScalingRole(conf.getEMRAutoscalingRole())
                .logUri("s3://"+conf.getEmrLogsS3ParamValue()+"-"+this.getAccount()+"/")//logsBucket.getARN()
                .visibleToAllUsers(Boolean.TRUE)
                .tags(List.of(emrTags))
                .jobFlowRole(conf.getEMRClusterRole())
                //.securityConfiguration(JsonPath.stringAt("$.Name"))
                .build();

        StringParameter emrClusterId = StringParameter.Builder.create(this, "emrClusterId")
                .parameterName(conf.getLambdaPrefixParamValue() + "/emrClusterId")
                .stringValue(Fn.ref(emrCluster.getLogicalId()))
                .build();

        //https://constructs.dev/packages/cdk-nag/v/2.18.42/api/NagPackSuppression?lang=java
        //Adding suppression for InTransit and AtRest encryption which is already in place through LZA
        NagPackSuppression nagPack1 =  NagPackSuppression.builder()//.appliesTo(List.of(emrCluster))
                .reason("InTransitEncryption in this context means very specific features within the different applications.\n" +
                        "For spark, InTransitEncryption only affects the on-cluster UIs that aren't accessible in our\n" +
                        "configurations.")
                .id("AwsSolutions-EMR5")
                .build();

        NagPackSuppression nagPack2 =  NagPackSuppression.builder()//.appliesTo(List.of(emrCluster))
                .reason("EBS-level encryption is already enabled by LZA through the defaultEBSEncryption attribute in global-config.yaml")
                .id("AwsSolutions-EMR4")
                .build();

        //https://constructs.dev/packages/cdk-nag/v/2.18.42/api/NagSuppressions?lang=java#addResourceSuppressions
        //Suppressing securityConfig for EMR clusters - this is already in place through LZA
        NagSuppressions.addResourceSuppressions(emrCluster, List.of(nagPack1, nagPack2));
        return emrCluster;
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
                .stringValue(conf.getAudits3ParamValue() + "-" + this.getAccount())
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
                .stringValue(conf.getDBBackupParamValue() + "-" + this.getAccount())
                .build();

        StringParameter dropDBParam = StringParameter.Builder.create(this, "dropDBParam")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getDropDBParamName())
                .stringValue(conf.getDropDBParamValue())
                .build();

        StringParameter entities = StringParameter.Builder.create(this, "entities")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getEntitiesParamName())
                .stringValue(conf.getEntitiesParamValue())
                .build();

        StringParameter entitiesChanges = StringParameter.Builder.create(this, "entityChangeDynamoDBName")
                .parameterName(conf.getLambdaPrefixParamValue() + "/entityChangeDynamoDBName")
                .stringValue(conf.getEntitiesParamValue()+"-changes")
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
                .stringValue(conf.getStoredProcSQLBucketParamValue() + "-" + this.getAccount())
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

        StringParameter secretsEndpoint = StringParameter.Builder.create(this, "secretsEndpoint")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getSecretsEndpointParamName())
                .stringValue(conf.getSecretsEndpointParamValue())
                .build();

        StringParameter batchSize = StringParameter.Builder.create(this, "batchSize")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getBatchSizeParamName())
                .stringValue(conf.getBatchSizeParamValue())
                .build();

        StringParameter emrLogBucketName = StringParameter.Builder.create(this, "emrLogBucketName")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getEmrLogsS3ParamName())
                .stringValue(conf.getEmrLogsS3ParamValue())
                .build();

        StringParameter reconLambdaBucketName = StringParameter.Builder.create(this, "reconLambdaBucketName")
                .parameterName(conf.getLambdaPrefixParamValue() + conf.getReconLambdaS3ParamName())
                .stringValue(conf.getReconLambdaS3ParamValue() + "-" + this.getAccount())
                .build();

    }
}
