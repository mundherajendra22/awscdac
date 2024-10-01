package com.citizens;


import software.amazon.awscdk.Stack;
import software.amazon.awscdk.*;
import software.amazon.awscdk.services.ec2.*;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.SfnStateMachine;
import software.amazon.awscdk.services.iam.FromRoleArnOptions;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.lambda.*;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.ITopic;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.stepfunctions.*;
import software.amazon.awscdk.services.stepfunctions.tasks.*;
import software.amazon.awscdk.services.ssm.StringParameter;
import software.amazon.awscdk.services.ssm.StringParameterAttributes;
import software.constructs.Construct;

import java.io.IOException;
import java.util.Map;
import java.util.*;

/**
 CDK Stack that sets up the required infra for AWS CDA Connector using stepfunctions for permanent clusters

 Author: Jay Mehta
 * */
public class UtilizedEMRStack extends Stack {
    private final Configuration conf;
    private final String environment;
    private final String stackName;

    public UtilizedEMRStack(final Construct parent, final String id) throws IOException {
        this(parent, id, null);
    }

    public UtilizedEMRStack(Construct scope, String id, StackProps props) throws IOException {
        super(scope, id, props);
        this.environment = (String) getNode().tryGetContext("environment");
        this.conf = new Configuration(environment);
        this.stackName = id;


        // These are the spark arguments that are passed to the EMR - this is where you define how your spark applications runs
        List<String> sparkArgs = Arrays.asList(
                conf.getSparkArgs().split(",")
        );

        //Getting EMR Cluster in a map to be used as a parameter
        Map clusterIDMap = new HashMap<String, String>();

        String clusterId =  StringParameter.fromStringParameterAttributes(this, "emrClusterId", StringParameterAttributes.builder()
                .parameterName(conf.getLambdaPrefixParamValue() + "/emrClusterId")
                .build()).getStringValue();

        clusterIDMap.put("ClusterId", clusterId);

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/stepfunctions/tasks/CallAwsService.html
        //Step to get cluster using EMR API
        CallAwsService emrClusterStatus = CallAwsService.Builder.create(this, "Describe EMR Cluster")
                .service("EMR")
                .action("describeCluster")
                .parameters(clusterIDMap)
                .iamResources(List.of("*"))
                .resultPath("$.TaskResult").build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/stepfunctions/tasks/EmrAddStep.html
        // A Step Functions Task to add a Step to an EMR Cluster. This step is applicable when existing cluster can be used
        EmrAddStep submitJob = EmrAddStep.Builder.create(this, "Submit_Job")
                .clusterId(clusterId)
                .name(stackName)
                .jar("command-runner.jar")
                .args(sparkArgs)
                .actionOnFailure(ActionOnFailure.CONTINUE)
                .integrationPattern(IntegrationPattern.RUN_JOB)
                .resultPath(JsonPath.DISCARD)
                .build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/stepfunctions/tasks/EmrAddStep.html
        // A Step Functions Task to add a Step to an EMR Cluster. This step is applicable when a new cluster is created
        EmrAddStep submitJobAfterCreation = EmrAddStep.Builder.create(this, "Submit_Job_After_Creating_Cluster")
                .clusterId(JsonPath.stringAt("$.ClusterId"))
                .name(stackName)
                .jar("command-runner.jar")
                .args(sparkArgs)
                .actionOnFailure(ActionOnFailure.CONTINUE)
                .integrationPattern(IntegrationPattern.RUN_JOB)
                .resultPath(JsonPath.DISCARD)
                .build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/sns/ITopic.html
        //Adding a SNS Topic
        String topicArn = "arn:aws:sns:" + this.getRegion() + ":" + this.getAccount() + ":" + conf.getSNSTopic();
        ITopic topic = Topic.fromTopicArn(this, "SNSTopic", topicArn);

        HashMap<String, Object> sparkErrorMessageMap = new HashMap<>();
        sparkErrorMessageMap.put("Error", JsonPath.stringAt("$.SparkFailure.Error"));
        sparkErrorMessageMap.put("Cause", JsonPath.stringAt("$.SparkFailure.Cause"));
        TaskInput sparkErrorMessage = TaskInput.fromObject(sparkErrorMessageMap);

        HashMap<String, Object> errorMessageMap = new HashMap<>();
        errorMessageMap.put("Error", JsonPath.stringAt("$.Error"));
        errorMessageMap.put("Cause", JsonPath.stringAt("$.Cause"));
        TaskInput errorMessage = TaskInput.fromObject(errorMessageMap);

        // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-stepfunctions-tasks.SnsPublish.html
        // A Step Functions Task to publish messages to SNS topic.
        SnsPublish notifyJobFailure = SnsPublish.Builder.create(this, "Notify_Job_Failure")
                .subject(stackName + " spark job has failed")
                .topic(topic)
                .message(sparkErrorMessage)
                .resultPath(JsonPath.DISCARD)
                .build();

        SnsPublish notifyReconFailure = SnsPublish.Builder.create(this, "Notify_Audit_&_Controls_Failure")
                .subject(stackName + " recon lambda has failed")
                .topic(topic)
                .message(errorMessage)
                .resultPath(JsonPath.DISCARD)
                .build();

        SnsPublish notifyClusterFailure = SnsPublish.Builder.create(this, "Notify_Cluster_Failure")
                .subject(stackName + " cluster has failed")
                .topic(topic)
                .message(errorMessage)
                .build();

        SnsPublish notifyClusterCreation = SnsPublish.Builder.create(this, "Notify_Cluster_Creation")
                .subject(stackName + " - new cluster is being created. Cluster with ID: "+clusterId+" is " +
                        "not available. Please get the latest ClusterID from EMR Management Console " +
                        "and update it in cdk.properties for this environment and re-deploy " +
                        "the "+stackName+ " stack to avoid creating multiple clusters")
                .topic(topic)
                .message(errorMessage)
                .build();

        SnsPublish notifyCompletion = SnsPublish.Builder.create(this, "Notify_Completion")
                .subject(stackName + " has completed")
                .topic(topic)
                .message(TaskInput.fromText("All Steps Completed Successfully!"))
                .resultPath(JsonPath.DISCARD)
                .build();

        SnsPublish notifyNoRun = SnsPublish.Builder.create(this, "Notify_NoRun")
                .subject(stackName + " has completed")
                .topic(topic)
                .message(TaskInput.fromText("No Steps were run as Launch Conditions were not met!"))
                .resultPath(JsonPath.DISCARD)
                .build();

        SnsPublish notifyLCFailure = SnsPublish.Builder.create(this, "Notify_LaunchCondition_Failure")
                .subject(stackName + " LaunchConditions lambda has failed")
                .topic(topic)
                .message(errorMessage)
                .resultPath(JsonPath.DISCARD)
                .build();
				
        SnsPublish notifyMergeViewFailure = SnsPublish.Builder.create(this, "Notify_MergedView_Failure")
                .subject(stackName + " merged view stored procedure has failed")
                .topic(topic)
                .message(errorMessage)
                .resultPath(JsonPath.DISCARD)
                .build();

				
		//Getting EMR Managed Scaling policy in a map to be used as a parameter
        Map computeLimits = new HashMap<String, String>();
        computeLimits.put("MaximumCapacityUnits", 80);
        computeLimits.put("MinimumCapacityUnits", 3);
        computeLimits.put("UnitType","Instances");

        Map managedPolicy = new HashMap<String, Map<String, String>>();
        managedPolicy.put("ComputeLimits", computeLimits);

        Map emrManagedPolicyParams = new HashMap<String, Map<String, Map<String, String>>>();;
        emrManagedPolicyParams.put("ClusterId", clusterId);
        emrManagedPolicyParams.put("ManagedScalingPolicy", managedPolicy);

        CallAwsService emrPutManagedScaling = CallAwsService.Builder.create(this, "PutManagedScaling for EMR Cluster")
                .service("EMR")
                .action("putManagedScalingPolicy")
                .parameters(emrManagedPolicyParams)
                .iamResources(List.of("*"))
                .resultPath("$.TaskResult").build();

        //Required for the new cluster incase old one is terminated
        CallAwsService emrNewPutManagedScaling = CallAwsService.Builder.create(this, "PutManagedScaling for new EMR Cluster")
                .service("EMR")
                .action("putManagedScalingPolicy")
                .parameters(emrManagedPolicyParams)
                .iamResources(List.of("*"))
                .resultPath("$.TaskResult").build();
        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/ec2/Vpc.html
        // VPC where the lambda functions should be deployed
        // TODO: Make sure the tgw attach subnets are removed when using LZA
        VpcLookupOptions vpcLookupOptions = VpcLookupOptions.builder().vpcId(conf.getVPCId()).build();
        IVpc vpc = Vpc.fromLookup(this, "VPC ID", vpcLookupOptions);

        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/Role.html
        // IAM role to be used by Lambda functions - RDSLambdaRole for functions that access RDS
        // and ReconLambdaRole for functions that access s3 and dynamodb
        IRole rdsLambdaRole = Role.fromRoleArn(this, "RDSLambdaRole",
                String.format("arn:aws:iam::%s:role/%s", this.getAccount(), conf.getRDSLambdaRole()),
                FromRoleArnOptions.builder().mutable(false).build());

        IRole reconLambdaRole = Role.fromRoleArn(this, "ReconLambdaRole",
                String.format("arn:aws:iam::%s:role/%s", this.getAccount(), conf.getReconLambdaRole()),
                FromRoleArnOptions.builder().mutable(false).build());

        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/s3/Bucket.html
        // S3 bucket where the lambda jar has been uploaded
        IBucket lambdaBucket = Bucket.fromBucketName(this, "LambdaS3Bucket", conf.getLambdaBucket() + '-' + this.getAccount());

        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/lambda/Function.html
        // Lambda function to run audit & controls
        Function functionOne = new Function(this, "RunAudit&Controls", FunctionProps.builder()
                .runtime(Runtime.JAVA_11)
                .code(Code.fromBucket(lambdaBucket, conf.getReconLambdaJar()))
                .handler(conf.getReconLambdaHandler())
                .role(reconLambdaRole)
                .memorySize(3008)
                .description("Audit & Controls lambda function updated on :"+System.currentTimeMillis())
                .timeout(Duration.minutes(15))
                .vpc(vpc)
                .allowPublicSubnet(Boolean.TRUE)
                .build());

        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/stepfunctions/tasks/LambdaInvoke.html
        // Invoke lambda function from StepFunctions as a task
        LambdaInvoke funcOne = LambdaInvoke.Builder.create(this, "Run Audit & Controls")
                .lambdaFunction(functionOne)
                .build();

        //Lambda function to create audit & controls report in a s3 bucket
        Function functionTwo = new Function(this, "Audit&ControlsReport", FunctionProps.builder()
                .runtime(Runtime.JAVA_11)
                .code(Code.fromBucket(lambdaBucket, conf.getReconLambdaJar()))
                .handler(conf.getReconReportLambdaHandler())
                .role(reconLambdaRole)
                .memorySize(3008)
                .description("Create Audit & Controls report lambda function updated on :"+System.currentTimeMillis())
                .timeout(Duration.minutes(15))
                .vpc(vpc)
                .allowPublicSubnet(Boolean.TRUE)
                .build());

        LambdaInvoke funcTwo = LambdaInvoke.Builder.create(this, "Create Audit & Controls Report")
                .lambdaFunction(functionTwo)
                .payload(TaskInput.fromText("1"))
                .build();

        //Lambda function that checks launch conditions for EMR Clusters. As a best practice, launch EMR Clusters when
        // the S3 bucket manifest has been updated.
        Function functionSix = new Function(this, "CheckCDAChanges", FunctionProps.builder()
                .runtime(Runtime.JAVA_11)
                .code(Code.fromBucket(lambdaBucket, conf.getReconLambdaJar()))
                .handler(conf.getCDAChangeHandler())
                .role(reconLambdaRole)
                .memorySize(3008)
                .timeout(Duration.minutes(15))
                .description("CDAC Launch conditions check lamnbda function updated on :"+System.currentTimeMillis())
                .vpc(vpc)
                .allowPublicSubnet(Boolean.TRUE)
                .build());

        LambdaInvoke funcSix = LambdaInvoke.Builder.create(this, "Check CDA Changes")
                .lambdaFunction(functionSix)
                .payload(TaskInput.fromText("1"))
                .outputPath("$.Payload")
                .build();

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/ec2/Subnet.html
        //Subnet where lambdas will be deployed
        ISubnet lambdaSubnet = Subnet.fromSubnetId(this, "LambdaSubnet", conf.getEMRSubnetId());

        Function functionSeven = new Function(this, "CallStoredProcedure", FunctionProps.builder()
                .runtime(Runtime.JAVA_11)
                .code(Code.fromBucket(lambdaBucket, conf.getReconLambdaJar()))
                .handler(conf.getStoredProcedureHandler())
                .role(reconLambdaRole)
                .memorySize(3008)
                .timeout(Duration.minutes(15))
                .description("MergedViews stored procedure lambda updated on :"+System.currentTimeMillis())
                .vpc(vpc)
                .allowPublicSubnet(Boolean.TRUE)
                .vpcSubnets(SubnetSelection.builder().subnets(List.of(lambdaSubnet)).build())
                .build());

        LambdaInvoke funcSeven = LambdaInvoke.Builder.create(this, "Call Stored Procedure")
                .lambdaFunction(functionSeven)
                .payload(TaskInput.fromText("1"))
                .outputPath("$.Payload")
                .build();

        //Lambda function to create schema in RDS. This function should typically be executed only once to create db
        // in RDS unless otherwise needed. It has to be executed manually from Lambda console with no input parameters
        Function functionEight = new Function(this, "CreateSchema", FunctionProps.builder()
                .runtime(Runtime.JAVA_11)
                .code(Code.fromBucket(lambdaBucket, conf.getReconLambdaJar()))
                .handler(conf.getDBSetUpLambdaHandler())
                .role(rdsLambdaRole)
                .memorySize(3008)
                .timeout(Duration.minutes(15))
                .description("Create Schema for RDS Lambda updated on :"+System.currentTimeMillis())
                .vpc(vpc)
                .allowPublicSubnet(Boolean.TRUE)
                .vpcSubnets(SubnetSelection.builder().subnets(List.of(lambdaSubnet)).build())
                .build());

        LambdaInvoke funcEight = LambdaInvoke.Builder.create(this, "Create Schema")
                .lambdaFunction(functionEight)
                .payload(TaskInput.fromText("1"))
                .build();


        HashMap<String, String> resultSelectorMap = new HashMap<>();
        resultSelectorMap.put("BatchRun.$","States.StringToJson($.Payload)");

        //Lambda function to create execution batches
        Function functionNine = new Function(this, "BatchManifestEntity", FunctionProps.builder()
                .runtime(Runtime.JAVA_11)
                .code(Code.fromBucket(lambdaBucket, conf.getReconLambdaJar()))
                .handler(conf.getBatchManifestHandler())
                .role(reconLambdaRole)
                .memorySize(3008)
                .timeout(Duration.minutes(15))
                .vpc(vpc)
                .allowPublicSubnet(Boolean.TRUE)
                .description("Batch manifest entity lambda updated on :"+System.currentTimeMillis())
                .vpcSubnets(SubnetSelection.builder().subnets(List.of(lambdaSubnet)).build())
                .build());

        LambdaInvoke funcNine = LambdaInvoke.Builder.create(this, "Batch Manifest Entity")
                .lambdaFunction(functionNine)
                .payload(TaskInput.fromText("1"))
                .resultSelector(resultSelectorMap)
                .build();

        //Lambda function to create execution batches
        Function functionTen = new Function(this, "CheckCDAEntityChanges", FunctionProps.builder()
                .runtime(Runtime.JAVA_11)
                .code(Code.fromBucket(lambdaBucket, conf.getReconLambdaJar()))
                .handler(conf.getCDAEntityChangeHandler())
                .role(reconLambdaRole)
                .memorySize(3008)
                .timeout(Duration.minutes(15))
                .vpc(vpc)
                .allowPublicSubnet(Boolean.TRUE)
                .description("Check CDA changed entities :"+System.currentTimeMillis())
                .vpcSubnets(SubnetSelection.builder().subnets(List.of(lambdaSubnet)).build())
                .build());

        // Create payload for Check CDA Entity chnages Lambda function
        HashMap<String, String> checkCDAEntityChangesPayloadMap = new HashMap<>();
        checkCDAEntityChangesPayloadMap.put("stepFunctionExecutionId.$","$$.Execution.Id");
        checkCDAEntityChangesPayloadMap.put("stepFunctionStartExecution.$", "$$.Execution.StartTime");
        TaskInput checkCDAEntityChangesPayload = TaskInput.fromObject(checkCDAEntityChangesPayloadMap);

        LambdaInvoke funcTen = LambdaInvoke.Builder.create(this, "Check CDA Entity changes")
                .lambdaFunction(functionTen)
                .payload(checkCDAEntityChangesPayload)
                .resultSelector(resultSelectorMap)
                .build();

        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/stepfunctions/Wait.html
        //Wait step - hold step function execution for 180s for DBInstance to be stopped
        Wait waitforCluster = Wait.Builder.create(this, "Wait 180 Seconds for Cluster to be in WAITING State")
                .time(WaitTime.duration(Duration.seconds(180)))
                .build();

        // Map for iteration Lambda run
        software.amazon.awscdk.services.stepfunctions.Map iteratorEntry = software.amazon.awscdk.services.stepfunctions.Map.Builder.create(this, "Map State")
                .itemsPath(JsonPath.stringAt("$.BatchRun"))
                .build();

        iteratorEntry.addCatch(notifyReconFailure, CatchProps.builder().errors(Collections.singletonList(Errors.ALL)).build());
        iteratorEntry.iterator(funcOne);

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/stepfunctions/Chain.html
        // Job failure chain
        Chain jobFailureChain = Chain.start(notifyJobFailure);

        //Common chain for audit & controls report & merged view creation
        Chain auditControlsChain = Chain.start(funcTen.addCatch(notifyReconFailure,
                                          CatchProps.builder()
                                               .errors(Collections.singletonList(Errors.ALL)).build()))
                .next(funcNine.addCatch(notifyReconFailure,
                CatchProps.builder()
                        .errors(Collections.singletonList(Errors.ALL)).build()))
                .next (iteratorEntry)

                .next(funcTwo.addCatch(notifyReconFailure,
                        CatchProps.builder()
                                .errors(Collections.singletonList(Errors.ALL)).build()))
				.next(funcSeven.addCatch(notifyMergeViewFailure,
                        CatchProps.builder()
							.errors(Collections.singletonList(Errors.ALL)).build()))
                .next(notifyCompletion);

        //Job chain that will submit job to EMR cluster
        Chain jobChain = Chain
		        .start(emrPutManagedScaling.addCatch(notifyClusterFailure,
                        CatchProps.builder().errors(Collections.singletonList(Errors.ALL)).build()))
                .next(submitJob.addCatch(jobFailureChain,
                        CatchProps.builder()
                                .resultPath("$.SparkFailure")
                                .errors(Collections.singletonList(Errors.ALL)).build()))
                .next(auditControlsChain);

        //Create EMR cluster incase existing one is not available
        Chain createClusterChain = Chain.start(notifyClusterCreation).next(createEMRCluster().addCatch(notifyClusterFailure,
                CatchProps.builder().errors(Collections.singletonList(Errors.ALL)).build()))
				.next(emrNewPutManagedScaling.addCatch(notifyClusterFailure,
                        CatchProps.builder().errors(Collections.singletonList(Errors.ALL)).build()))
                .next(submitJobAfterCreation.addCatch(jobFailureChain,
                        CatchProps.builder()
                                .resultPath("$.SparkFailure")
                                .errors(Collections.singletonList(Errors.ALL)).build()))
                .next(auditControlsChain);

        //Chain that runs CDAC after launch conditions have been verified
        Chain postLaunchConditions = Chain.start(emrClusterStatus).next(
                new Choice(this, "Is Cluster Available at Start?")
                        .when(Condition.stringEquals("$.TaskResult.Cluster.Status.State", "WAITING"), jobChain)
                        .when(Condition.or(
                                        Condition.stringEquals("$.TaskResult.Cluster.Status.State", "TERMINATED"),
                                        Condition.stringEquals("$.TaskResult.Cluster.Status.State", "TERMINATING"),
                                        Condition.stringEquals("$.TaskResult.Cluster.Status.State", "TERMINATED_WITH_ERRORS")),
                                createClusterChain
                        )
                        .otherwise(waitforCluster.next(emrClusterStatus))
        );

        //Main Chain that checks if there are data changes to start CDAC run
        Chain chain = Chain.start(funcSix.addCatch(notifyLCFailure, CatchProps.builder().errors(Collections.singletonList(Errors.ALL)).build()))
                .next(new Choice(this, "Can CDAC Start?").otherwise(notifyNoRun).
                        when(Condition.stringEquals("$.status", "START"), postLaunchConditions));


        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/Role.html
        // IAM role to be used by StepFunctions State Machine
        IRole stepFnRole = Role.fromRoleArn(this, "StepFunctionsRole",
                String.format("arn:aws:iam::%s:role/%s", this.getAccount(), conf.getEMRStepFunctionRole()),
                FromRoleArnOptions.builder().mutable(false).build());

        // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-stepfunctions.StateMachine.html
        // Define a StepFunctions State Machine.
        StateMachine stepFunction = StateMachine.Builder.create(this, "StepFunction")
                .stateMachineName(stackName)
                .definition(chain)
                .logs(LogOptions.builder().level(LogLevel.ALL).includeExecutionData(true).destination(new LogGroup(this, "cdac/utilizedemr")).build())
                .tracingEnabled(true)
                .role(stepFnRole)
                .build();

        // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-events-targets.SfnStateMachine.html
        // Use a StepFunctions state machine as a target for Amazon EventBridge rules.
        SfnStateMachine sfnStateMachine = SfnStateMachine.Builder.create(stepFunction)
                .build();

        // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-events.Rule.html
        // Defines an EventBridge Rule in this stack.
        Rule.Builder.create(this, stackName)
                .ruleName(stackName)
                .targets(Collections.singletonList(sfnStateMachine))
                .schedule(Schedule.expression(String.format("cron(%s)", conf.getCronExpression())))
                .enabled(false)
                .build();

        StringParameter stepFunctionArn = StringParameter.Builder.create(this, "stepFunctionArn")
                .parameterName(conf.getLambdaPrefixParamValue() + "/stepFunctionArn")
                .stringValue(stepFunction.getStateMachineArn())
                .build();

       Tags.of(this).add("Team", "data-engineering");
    }

    private EmrCreateCluster createEMRCluster() {
        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/stepfunctions/tasks/EmrCreateCluster.AutoScalingPolicyProperty.html
        // Autoscaling policy to be applied to EMR Clusters based on CloudWatch metric
        // Uses yarn memory available metric to scale out clusters
        /*EmrCreateCluster.ScalingRuleProperty scaleOutRule = EmrCreateCluster.ScalingRuleProperty.builder()
                .action(EmrCreateCluster.ScalingActionProperty.builder()
                        .simpleScalingPolicyConfiguration(EmrCreateCluster.SimpleScalingPolicyConfigurationProperty.builder()
                                .scalingAdjustment(2)
                                // the properties below are optional
                                .adjustmentType(EmrCreateCluster.ScalingAdjustmentType.CHANGE_IN_CAPACITY)
                                .coolDown(300)
                                .build())
                        // the properties below are optional
                        //.market(EmrCreateCluster.InstanceMarket.ON_DEMAND)
                        .build())
                .name("AutoScaleOutPolicy")
                .trigger(EmrCreateCluster.ScalingTriggerProperty.builder()
                        .cloudWatchAlarmDefinition(EmrCreateCluster.CloudWatchAlarmDefinitionProperty.builder()
                                .comparisonOperator(EmrCreateCluster.CloudWatchAlarmComparisonOperator.LESS_THAN_OR_EQUAL)
                                .metricName("YARNMemoryAvailablePercentage")
                                .period(Duration.minutes(5))
                                // the properties below are optional
                                .dimensions(Arrays.asList(EmrCreateCluster.MetricDimensionProperty.builder()
                                        .key("JobFlowId")
                                        .value("${emr.clusterID}")
                                        .build()))
                                .evaluationPeriods(1)
                                .namespace("AWS/ElasticMapReduce")
                                .statistic(EmrCreateCluster.CloudWatchAlarmStatistic.AVERAGE)
                                .threshold(40)
                                .unit(EmrCreateCluster.CloudWatchAlarmUnit.PERCENT)
                                .build())
                        .build())
                // the properties below are optional
                .description("ScaleOut Policy")
                .build();

        EmrCreateCluster.ScalingRuleProperty scaleInRule = EmrCreateCluster.ScalingRuleProperty.builder()
                .action(EmrCreateCluster.ScalingActionProperty.builder()
                        .simpleScalingPolicyConfiguration(EmrCreateCluster.SimpleScalingPolicyConfigurationProperty.builder()
                                .scalingAdjustment(1)
                                // the properties below are optional
                                .adjustmentType(EmrCreateCluster.ScalingAdjustmentType.CHANGE_IN_CAPACITY)
                                .coolDown(300)
                                .build())
                        // the properties below are optional
                        //.market(EmrCreateCluster.InstanceMarket.ON_DEMAND)
                        .build())
                .name("AutoScaleInPolicy")
                .trigger(EmrCreateCluster.ScalingTriggerProperty.builder()
                        .cloudWatchAlarmDefinition(EmrCreateCluster.CloudWatchAlarmDefinitionProperty.builder()
                                .comparisonOperator(EmrCreateCluster.CloudWatchAlarmComparisonOperator.GREATER_THAN_OR_EQUAL)
                                .metricName("YARNMemoryAvailablePercentage")
                                .period(Duration.minutes(5))
                                // the properties below are optional
                                .dimensions(Arrays.asList(EmrCreateCluster.MetricDimensionProperty.builder()
                                        .key("JobFlowId")
                                        .value("${emr.clusterID}")
                                        .build()))
                                .evaluationPeriods(1)
                                .namespace("AWS/ElasticMapReduce")
                                .statistic(EmrCreateCluster.CloudWatchAlarmStatistic.AVERAGE)
                                .threshold(90)
                                .unit(EmrCreateCluster.CloudWatchAlarmUnit.PERCENT)
                                .build())
                        .build())
                // the properties below are optional
                .description("ScaleIn Policy")
                .build();

        EmrCreateCluster.AutoScalingPolicyProperty autoScalingPolicyProperty = EmrCreateCluster.AutoScalingPolicyProperty.builder()
                .constraints(EmrCreateCluster.ScalingConstraintsProperty.builder()
                        .maxCapacity(10)
                        .minCapacity(1)
                        .build())
                .rules(Arrays.asList(scaleOutRule, scaleInRule))
                .build();
				*/

        // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-stepfunctions-tasks.EmrCreateCluster.InstanceGroupConfigProperty.html
        // Configuration defining a new instance group.
        EmrCreateCluster.InstanceGroupConfigProperty masterConfigGroup = EmrCreateCluster.InstanceGroupConfigProperty
                .builder()
                .instanceCount(conf.getEMRMasterInstanceCount())
                .instanceRole(EmrCreateCluster.InstanceRoleType.MASTER)
                .name("Master")
                .instanceType(conf.getEMRMasterInstanceType())
                //.autoScalingPolicy(autoScalingPolicyProperty)
                //.market(EmrCreateCluster.InstanceMarket.ON_DEMAND)
                .build();
        EmrCreateCluster.InstanceGroupConfigProperty coreConfigGroup = EmrCreateCluster.InstanceGroupConfigProperty
                .builder()
                .instanceCount(conf.getEMRCoreInstanceCount())
                .instanceRole(EmrCreateCluster.InstanceRoleType.CORE)
                .name("Core")
                .instanceType(conf.getEMRCoreInstanceType())
                //.autoScalingPolicy(autoScalingPolicyProperty)
                //.market(EmrCreateCluster.InstanceMarket.ON_DEMAND)
                .build();
        EmrCreateCluster.InstanceGroupConfigProperty taskConfigGroup = EmrCreateCluster.InstanceGroupConfigProperty
                .builder()
                .instanceCount(conf.getEMRTaskInstanceCount())
                .instanceRole(EmrCreateCluster.InstanceRoleType.TASK)
                .name("Task")
                .instanceType(conf.getEMRTaskInstanceType())
                //.autoScalingPolicy(autoScalingPolicyProperty)
                //.market(EmrCreateCluster.InstanceMarket.ON_DEMAND)
                .build();

        final ArrayList<EmrCreateCluster.InstanceGroupConfigProperty> instanceGroups = new ArrayList<>();
        instanceGroups.add(masterConfigGroup);
        instanceGroups.add(coreConfigGroup);
        instanceGroups.add(taskConfigGroup);

        // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-stepfunctions-tasks.EmrCreateCluster.InstancesConfigProperty.html
        // A specification of the number and type of Amazon EC2 instances.
        EmrCreateCluster.InstancesConfigProperty instanceConfig = EmrCreateCluster.InstancesConfigProperty.builder()
                .ec2SubnetId(conf.getEMRSubnetId())
                .ec2KeyName(conf.getEMREc2KeyName())
                .instanceGroups(instanceGroups)
                .terminationProtected(Boolean.TRUE).build();

        // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-stepfunctions-tasks.EmrCreateCluster.ApplicationConfigProperty.html
        // Properties for the EMR Cluster Applications.
        final List<EmrCreateCluster.ApplicationConfigProperty> appList = new ArrayList<>();
        EmrCreateCluster.ApplicationConfigProperty sparkApp = new EmrCreateCluster.ApplicationConfigProperty.Builder()
                .name("Spark").build();
        appList.add(sparkApp);


        // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-iam.IRole.html
        // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-iam.Role.html#static-fromwbrrolewbrarnscope-id-rolearn-options
        IRole emrClusterRole = Role.fromRoleArn(this, "EMR_Cluster_Role",
                String.format("arn:aws:iam::%s:role/%s", this.getAccount(), conf.getEMRClusterRole()),
                FromRoleArnOptions.builder().mutable(false).build());
        IRole emrServiceRole = Role.fromRoleArn(this, "EMR_Service_Role",
                String.format("arn:aws:iam::%s:role/%s", this.getAccount(), conf.getEMRServiceRole()),
                FromRoleArnOptions.builder().mutable(false).build());
        IRole emrAutoscalingRole = Role.fromRoleArn(this, "EMR_Autoscaling_Role",
                String.format("arn:aws:iam::%s:role/%s", this.getAccount(), conf.getEMRAutoscalingRole()),
                FromRoleArnOptions.builder().mutable(false).build());

        final HashMap<String, String> emrTags = new HashMap<>();
        emrTags.put("Name", conf.getEMRName());
        emrTags.put("Environment", environment);

        // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-stepfunctions-tasks.EmrCreateCluster.html
        // A Step Functions Task to create an EMR Cluster.
        return EmrCreateCluster.Builder.create(this, "Create_an_EMR_Cluster")
                .name(conf.getEMRName())
                .releaseLabel(conf.getEMRReleaseLabel())
                .instances(instanceConfig)
                .clusterRole(emrClusterRole)
                .serviceRole(emrServiceRole)
                .autoScalingRole(emrAutoscalingRole)
                .applications(appList)
                .logUri("arn:aws:s3:::"+conf.getEmrLogsS3ParamValue())
                .visibleToAllUsers(Boolean.TRUE)
                .tags(emrTags)
                .build();
    }
}
