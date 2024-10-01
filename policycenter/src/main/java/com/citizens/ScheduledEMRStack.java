package com.citizens;

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.Tags;
import software.amazon.awscdk.services.ec2.*;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.SfnStateMachine;
import software.amazon.awscdk.services.iam.FromRoleArnOptions;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.FunctionProps;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.ITopic;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.stepfunctions.*;
import software.amazon.awscdk.services.stepfunctions.tasks.*;
import software.constructs.Construct;

import java.io.IOException;
import java.util.Map;
import java.util.*;

/**
 CDK Stack that sets up the required infra for AWS CDA Connector using stepfunctions.

 Author: Jay Mehta
 * */
public class ScheduledEMRStack extends Stack {

    private final Configuration conf;
    private final String environment;
    private final String stackName;

    public ScheduledEMRStack(final Construct parent, final String id) throws IOException {
        this(parent, id, null);
    }

    public ScheduledEMRStack(final Construct parent, final String id, final StackProps props) throws IOException {
        super(parent, id, props);

        this.environment = (String) getNode().tryGetContext("environment");
        this.conf = new Configuration(environment);
        this.stackName = id;

        // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-stepfunctions-tasks.EmrCreateCluster.html
        // A Step Functions Task to create an EMR Cluster.
        EmrCreateCluster createCluster = createEMRCluster();

        // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-stepfunctions-tasks.EmrTerminateCluster.html
        // A Step Functions Task to terminate an EMR Cluster.
        EmrTerminateCluster terminateCluster = EmrTerminateCluster.Builder.create(this, "Terminate_EMR_Cluster")
                .clusterId(JsonPath.stringAt("$.ClusterId"))
                .build();

        // These are the spark arguments that are passed to the EMR - this is where you define how your spark applications runs
        List<String> sparkArgs = Arrays.asList(
                conf.getSparkArgs().split(",")
        );

        // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-stepfunctions-tasks.EmrAddStep.html
        // A Step Functions Task to add a Step to an EMR Cluster.
        EmrAddStep submitJob = EmrAddStep.Builder.create(this, "Submit_Job")
                .clusterId(JsonPath.stringAt("$.ClusterId"))
                .name(stackName)
                .jar("command-runner.jar")
                .args(sparkArgs)
                .actionOnFailure(ActionOnFailure.CONTINUE)
                .integrationPattern(IntegrationPattern.RUN_JOB)
                .resultPath(JsonPath.DISCARD)
                .build();

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
        SnsPublish notifyClusterFailure = SnsPublish.Builder.create(this, "Notify_Cluster_Failure")
                .subject(stackName + " cluster has failed")
                .topic(topic)
                .message(errorMessage)
                .build();

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

        SnsPublish notifyDbBackupFailure = SnsPublish.Builder.create(this, "Notify_DBBackUp_Failure")
                .subject(stackName + " dbbackup lambda has failed")
                .topic(topic)
                .message(errorMessage)
                .resultPath(JsonPath.DISCARD)
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

        //Getting DBInstanceIdentifier in a map to be used as a parameter
        Map descDBInstance = new HashMap<String, String>();
        descDBInstance.put("DbInstanceIdentifier", conf.getDBInstanceIdentifier());

        // https://docs.aws.amazon.com/cdk/api/v2/java/index.html?software/amazon/awscdk/services/stepfunctions/tasks/CallAwsService.html
        //Step to call RDS service API
        CallAwsService rdsDescribeAtStart = CallAwsService.Builder.create(this, "Check if DBInstance is available at Start?")
                .service("RDS")
                .action("describeDBInstances")
                .parameters(descDBInstance)
                .iamResources(List.of("*"))
                .resultPath("$.TaskResult").build();

        CallAwsService rdsStartDBInst = CallAwsService.Builder.create(this, "Start DBInstance")
                .service("RDS")
                .action("startDBInstance")
                .parameters(descDBInstance)
                .iamResources(List.of("*"))
                .resultPath("$.TaskResult").build();

        CallAwsService rdsDescribeStart = CallAwsService.Builder.create(this, "Check if DBInstance is available after start?")
                .service("RDS")
                .action("describeDBInstances")
                .parameters(descDBInstance)
                .iamResources(List.of("*"))
                .resultPath("$.TaskResult").build();

        CallAwsService rdsDescribeAtStop = CallAwsService.Builder.create(this, "DescribeDBInstances for Stopping?")
                .service("RDS")
                .action("describeDBInstances")
                .parameters(descDBInstance)
                .iamResources(List.of("*"))
                .resultPath("$.TaskResult").build();

        /*CallAwsService rdsStopDBInst = CallAwsService.Builder.create(this, "Stop DBInstance")
                .service("RDS")
                .action("stopDBInstance")
                .parameters(descDBInstance)
                .iamResources(List.of("*"))
                .resultPath("$.TaskResult").build();

         */

        descDBInstance.put("DbClusterIdentifier",conf.getDBInstanceIdentifier());
        descDBInstance.put("DbClusterSnapshotIdentifier.$","States.Format('gwcc-{}', States.UUID())");
        CallAwsService createDBClusterSnapShot = CallAwsService.Builder.create(this, "Create RDS DBCluster Snapshot")
                .service("RDS")
                .action("CreateDBClusterSnapshot")
                .parameters(descDBInstance)
                .iamResources(List.of("*"))
                .resultPath("$.dbClusterSnapshotIdentifier").build();
        descDBInstance.put("DbClusterSnapshotIdentifier.$","$.dbClusterSnapshotIdentifier.DbClusterSnapshot.DbClusterSnapshotIdentifier");

        CallAwsService descDBClusterSnapShot = CallAwsService.Builder.create(this, "Describe RDS DBCluster Snapshot")
                .service("RDS")
                .action("DescribeDBClusterSnapshots")
                .parameters(descDBInstance)
                .iamResources(List.of("*"))
                .resultPath("$.TaskResult").build();

        CallAwsService startExportTask = CallAwsService.Builder.create(this, "Export RDS Snapshot")
                .service("RDS")
                .action("CreateDBSnapshot")
                .parameters(descDBInstance)
                .iamResources(List.of("*"))
                .resultPath("$.TaskResult").build();

        //Getting EMR Managed Scaling policy in a map to be used as a parameter
        Map computeLimits = new HashMap<String, String>();
        computeLimits.put("MaximumCapacityUnits", 100);
        //computeLimits.put("MaximumCoreCapacityUnits", conf.getEMRCoreInstanceCount());
        //computeLimits.put("MaximumOnDemandCapacityUnits", conf.getEMRCoreInstanceCount());
        computeLimits.put("MinimumCapacityUnits", 4);
        computeLimits.put("UnitType","Instances");

        Map managedPolicy = new HashMap<String, Map<String, String>>();
        managedPolicy.put("ComputeLimits", computeLimits);

        Map emrManagedPolicyParams = new HashMap<String, Map<String, Map<String, String>>>();;
        emrManagedPolicyParams.put("ClusterId", conf.getEMRClusterId());
        emrManagedPolicyParams.put("ManagedScalingPolicy", managedPolicy);

        CallAwsService emrPutManagedScaling = CallAwsService.Builder.create(this, "PutManagedScaling for EMR Cluster")
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
        IBucket lambdaBucket = Bucket.fromBucketName(this, "LambdaS3Bucket", conf.getLambdaBucket());

        //https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/ec2/Subnet.html
        //Subnet where lambdas will be deployed
        ISubnet lambdaSubnet = Subnet.fromSubnetId(this, "LambdaSubnet", conf.getEMRSubnetId());

        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/lambda/Function.html
        // Lambda function to run audit & controls
        Function functionOne = new Function(this, "RunAudit&Controls", FunctionProps.builder()
                .runtime(Runtime.JAVA_11)
                .code(Code.fromBucket(lambdaBucket, conf.getReconLambdaJar()))
                .handler(conf.getReconLambdaHandler())
                .role(reconLambdaRole)
                .memorySize(3008)
                .timeout(Duration.minutes(15))
                .vpc(vpc)
                .vpcSubnets(SubnetSelection.builder().subnets(List.of(lambdaSubnet)).build())
                .build());

        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/stepfunctions/tasks/LambdaInvoke.html
        // Invoke lambda function from StepFunctions as a task
        LambdaInvoke funcOne = LambdaInvoke.Builder.create(this, "Run Audit & Controls")
                .lambdaFunction(functionOne)
                .payload(TaskInput.fromText("1"))
                .build();

        //Lambda function to create audit & controls report in a s3 bucket
        Function functionTwo = new Function(this, "Audit&ControlsReport", FunctionProps.builder()
                .runtime(Runtime.JAVA_11)
                .code(Code.fromBucket(lambdaBucket, conf.getReconLambdaJar()))
                .handler(conf.getReconReportLambdaHandler())
                .role(reconLambdaRole)
                .memorySize(3008)
                .timeout(Duration.minutes(15))
                .vpc(vpc)
                .vpcSubnets(SubnetSelection.builder().subnets(List.of(lambdaSubnet)).build())
                .build());

        LambdaInvoke funcTwo = LambdaInvoke.Builder.create(this, "Create Audit & Controls Report")
                .lambdaFunction(functionTwo)
                .payload(TaskInput.fromText("1"))
                .build();


        //Lambda function to create schema in RDS. This function should typically be executed only once to create db
        // in RDS unless otherwise needed. It has to be executed manually from Lambda console with no input parameters
        Function functionFive = new Function(this, "CreateSchema", FunctionProps.builder()
                .runtime(Runtime.JAVA_11)
                .code(Code.fromBucket(lambdaBucket, conf.getReconLambdaJar()))
                .handler(conf.getDBSetUpLambdaHandler())
                .role(rdsLambdaRole)
                .memorySize(3008)
                .timeout(Duration.minutes(15))
                .vpc(vpc)
                .vpcSubnets(SubnetSelection.builder().subnets(List.of(lambdaSubnet)).build())
                .build());

        LambdaInvoke funcFive = LambdaInvoke.Builder.create(this, "Create Schema")
                .lambdaFunction(functionFive)
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
                .vpc(vpc)
                .vpcSubnets(SubnetSelection.builder().subnets(List.of(lambdaSubnet)).build())
                .build());

        LambdaInvoke funcSix = LambdaInvoke.Builder.create(this, "Check CDA Changes")
                .lambdaFunction(functionSix)
                .payload(TaskInput.fromText("1"))
                .outputPath("$.Payload")
                .build();

        Function functionSeven = new Function(this, "CallStoredProcedure", FunctionProps.builder()
                .runtime(Runtime.JAVA_11)
                .code(Code.fromBucket(lambdaBucket, conf.getReconLambdaJar()))
                .handler(conf.getStoredProcedureHandler())
                .role(reconLambdaRole)
                .memorySize(3008)
                .timeout(Duration.minutes(15))
                .vpc(vpc)
                .vpcSubnets(SubnetSelection.builder().subnets(List.of(lambdaSubnet)).build())
                .build());

        LambdaInvoke funcSeven = LambdaInvoke.Builder.create(this, "Call Stored Procedure")
                .lambdaFunction(functionSeven)
                .payload(TaskInput.fromText("1"))
                .outputPath("$.Payload")
                .build();


        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/stepfunctions/Wait.html
        //Wait step - hold step function execution for 60s
        Wait waitX = Wait.Builder.create(this, "Wait 60 Seconds")
                .time(WaitTime.duration(Duration.seconds(60)))
                .build();

        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/stepfunctions/Wait.html
        //Wait step - hold step function execution for 180s for DBInstance to be available
        Wait waitforDBAvail = Wait.Builder.create(this, "Wait 180 Seconds to Start DBInstance")
                .time(WaitTime.duration(Duration.seconds(180)))
                .build();

        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/stepfunctions/Wait.html
        //Wait step - hold step function execution for 180s for DBInstance to be stopped
        /*Wait waitforDBStop = Wait.Builder.create(this, "Wait 180 Seconds to Stop DBInstance")
                .time(WaitTime.duration(Duration.seconds(180)))
                .build();

         */

        // https://docs.aws.amazon.com/cdk/api/v2/java/index.html?software/amazon/awscdk/services/stepfunctions/Chain.html
        // A collection of states to chain onto.
        //Job failure chain - that gets called onto when a step in EMR fails
        Chain jobFailureChain = Chain.start(notifyJobFailure).next(terminateCluster);

        //Job chain that stops DBInstance
        /*Chain dbInstanceStopChain = Chain.start(rdsStopDBInst).next(rdsDescribeAtStop).next(
                new Choice(this, "Is DBInstance Stopped?")
                        .when(Condition.stringEquals("$.TaskResult.DbInstances[0].DbInstanceStatus", "stopped"), notifyCompletion)
                        .otherwise(waitforDBStop.next(rdsDescribeAtStop))
        );

         */

        //Job chain that creates cluster, adds a step, terminates clusters, runs audit & controls,
        // creates audit & controls report, creates rds-sql server back up and sends completion notification
        Chain subChain = Chain.start(createCluster.addCatch(notifyClusterFailure,
                        CatchProps.builder().errors(Collections.singletonList(Errors.ALL)).build()))
                .next(emrPutManagedScaling.addCatch(notifyClusterFailure,
                        CatchProps.builder().errors(Collections.singletonList(Errors.ALL)).build()))
                .next(submitJob.addCatch(jobFailureChain,
                        CatchProps.builder()
                                .resultPath("$.SparkFailure")
                                .errors(Collections.singletonList(Errors.ALL)).build()))
                .next(terminateCluster.addCatch(notifyClusterFailure,
                        CatchProps.builder().errors(Collections.singletonList(Errors.ALL)).build()))
                .next(funcOne.addCatch(notifyReconFailure,
                        CatchProps.builder()
                                .errors(Collections.singletonList(Errors.ALL)).build()))
                .next(funcTwo.addCatch(notifyReconFailure,
                        CatchProps.builder()
                                .errors(Collections.singletonList(Errors.ALL)).build()))
                .next(funcSeven.addCatch(notifyMergeViewFailure,
                        CatchProps.builder().errors(Collections.singletonList(Errors.ALL)).build()))
                .next(notifyCompletion);


        // https://docs.aws.amazon.com/cdk/api/v2/java/software/amazon/awscdk/services/iam/Role.html
        // IAM role to be used by StepFunctions State Machine
        IRole stepFnRole = Role.fromRoleArn(this, "StepFunctionsRole",
                String.format("arn:aws:iam::%s:role/%s", this.getAccount(), conf.getEMRStepFunctionRole()),
                FromRoleArnOptions.builder().mutable(false).build());

        //Chain that starts DBInstance and checks for availability
        Chain dbInstanceStartChain = Chain.start(rdsStartDBInst.next(rdsDescribeStart.next(
                new Choice(this, "Is DBInstance Available?").otherwise(waitforDBAvail.next(rdsDescribeStart)).
                        when(Condition.stringEquals("$.TaskResult.DbInstances[0].DbInstanceStatus", "available"), subChain)
        )));

        //Chain that checks if DBInstance is available at Start
        Chain dbInstanceAtStartChain = Chain.start(rdsDescribeAtStart).next(
                new Choice(this, "Is DBInstance Available at Start?")
                        .when(Condition.stringEquals("$.TaskResult.DbInstances[0].DbInstanceStatus", "available"), subChain)
                        .otherwise(dbInstanceStartChain)
        );

        //Actual Job Chain that checks whether cdac can start and hooks onto the subchain
        Chain actualChain = Chain
                .start(funcSix.addCatch(notifyLCFailure, CatchProps.builder().errors(Collections.singletonList(Errors.ALL)).build()))
                .next(new Choice(this, "Can CDAC Start?").otherwise(notifyNoRun).
                        when(Condition.stringEquals("$.status", "START"), dbInstanceAtStartChain));

        // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-stepfunctions.StateMachine.html
        // Define a StepFunctions State Machine.
        StateMachine stepFunction = StateMachine.Builder.create(this, "StepFunction")
                .stateMachineName(stackName)
                .definition(actualChain)
                .logs(LogOptions.builder().level(LogLevel.ALL).includeExecutionData(true).destination(new LogGroup(this, "cdac/scehduledemr")).build())
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
                .terminationProtected(Boolean.FALSE).build();

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
        emrTags.put("Team", "data-engineering");

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