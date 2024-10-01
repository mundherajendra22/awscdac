package com.citizens;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 A bean class that reads properties from properties file for the respective environment
 This class will be used in the Stack to load configuration values

 Author: Jay Mehta
 * */

public class Configuration {

    private final String environment;
    private final Properties properties = new Properties();

    /**
     * Constructor that reads the properties file based on the environment passed in cdk args
     * @param environment
     * @throws IOException
     */
    public Configuration(String environment) throws IOException {
        this.environment = environment;
        String environmentDirectory = "environments" + System.getProperty("file.separator") + environment;
        try (InputStream input = Files.newInputStream(Paths.get(environmentDirectory + System.getProperty("file.separator") + "cdk.properties"))) {
            properties.load(input);
        }
    }

    /**
     * Function used to retrieve parameters from Properties file by passing in the key
     * @param key
     * @return
     */
    private String getOrFail(String key) {
        String value = properties.getProperty(key);
        if (value == null)
            throw new IllegalArgumentException(String.format("Property %s is not found in cdk.properties file", key));
        return value;
    }

    public String getEnvironment() {
        return environment;
    }

    public String getEMRSubnetId() {
        return getOrFail("emr.subnet.id");
    }

    public String getEMREc2KeyName() {
        return getOrFail("emr.ec2.key.name");
    }

    public int getEMRMasterInstanceCount() {
        return Integer.parseInt(getOrFail("emr.master.instance.count"));
    }

    public String getEMRMasterInstanceType() {
        return getOrFail("emr.master.instance.type");
    }

    public int getEMRCoreInstanceCount() {
        return Integer.parseInt(getOrFail("emr.core.instance.count"));
    }

    public String getEMRCoreInstanceType() {
        return getOrFail("emr.core.instance.type");
    }

    public int getEMRTaskInstanceCount() {
        return Integer.parseInt(getOrFail("emr.task.instance.count"));
    }

    public String getEMRTaskInstanceType() {
        return getOrFail("emr.task.instance.type");
    }

    public String getEMRReleaseLabel() {
        return getOrFail("emr.release.label");
    }

    public String getEMRName() {
        return getOrFail("emr.name");
    }

    public String getEMRClusterRole() {
        return getOrFail("emr.cluster.role");
    }

    public String getEMRAutoscalingRole() {
        return getOrFail("emr.autoscaling.role");
    }

    public String getEMRServiceRole() {
        return getOrFail("emr.service.role");
    }

    public String getEMRLogsBucketArn() {
        return getOrFail("emr.logs.bucket.arn");
    }

    public String getSNSTopic() {
        return getOrFail("sns.topic");
    }

    public String getCronExpression() {
        return getOrFail("cron.expression");
    }

    public String getSparkArgs() {
        return getOrFail("emr.spark.args");
    }

    public String getDBInstanceIdentifier(){ return getOrFail("rds.db.identifier"); }

    public String getEMRStepFunctionRole() {
        return getOrFail("emr.stepfunction.role");
    }

    public String getLambdaBucket() {
        return getOrFail("recon.lambda.bucket");
    }

    public String getReconLambdaHandler() {
        return getOrFail("recon.run.lambda.handler");
    }

    public String getReconReportLambdaHandler() {
        return getOrFail("recon.report.lambda.handler");
    }

    public String getReconLambdaJar() {
        return getOrFail("recon.lambda.jar");
    }

    public String getReconLambdaRole() {
        return getOrFail("recon.lambda.role");
    }

    public String getRDSLambdaRole() {
        return getOrFail("rds.lambda.role");
    }

    public String getDBBackUpLambdaHandler() {
        return getOrFail("dbbackup.lambda.handler");
    }

    public String getDBBackUpStatusLambdaHandler() {
        return getOrFail("dbbackup.status.lambda.handler");
    }

    public String getDBSetUpLambdaHandler() {
        return getOrFail("dbsetup.lambda.handler");
    }

    public String getCDAChangeHandler() {
        return getOrFail("cda.check.lambda.handler");
    }

    public String getStoredProcedureHandler() { return getOrFail("cda.storedprocrds.lambda.handler");}

    public String getVPCId() {
        return getOrFail("lambda.vpc.id");
    }

    public String getCustomerAccountId() {
        return getOrFail("customer.account.id");
    }

    public String getCustomerRegion() {
        return getOrFail("customer.account.region");
    }

    public String getEMRClusterId() {
        return getOrFail("emr.cluster.id");
    }

    public String getManifestParamName() {
        return getOrFail("manifestkey.paramname");
    }

    public String getManifestParamValue() {
        return getOrFail("manifestkey.paramvalue");
    }

    public String getLambdaPrefixParamName() {
        return getOrFail("lambdaprefix.paramname");
    }

    public String getLambdaPrefixParamValue() {
        return getOrFail("lambdaprefix.paramvalue");
    }

    public String getAudits3ParamName() {
        return getOrFail("cdac.audits3.paramname");
    }

    public String getAudits3ParamValue() {
        return getOrFail("cdac.audits3.paramvalue");
    }

    public String getCDAs3ParamName() {
        return getOrFail("cdac.cdas3.paramname");
    }

    public String getCDAs3ParamValue() {
        return getOrFail("cdac.cdas3.paramvalue");
    }

    public String getCDAs3EndpointParamName() {
        return getOrFail("cdac.cdas3endpoint.paramname");
    }

    public String getCDAs3EndpointParamValue() {
        return getOrFail("cdac.cdas3endpoint.paramvalue");
    }

    public String getDBBackupParamName() {
        return getOrFail("cdac.dbbackup.paramname");
    }

    public String getDBBackupParamValue() {
        return getOrFail("cdac.dbbackup.paramvalue");
    }

    public String getDropDBParamName() {
        return getOrFail("cdac.dropdb.paramname");
    }

    public String getDropDBParamValue() {
        return getOrFail("cdac.dropdb.paramvalue");
    }

    public String getEntitiesParamName() {
        return getOrFail("cdac.entities.paramname");
    }

    public String getEntitiesParamValue() {
        return getOrFail("cdac.entities.paramvalue");
    }

    public String getJdbcSchemaRawParamName() {
        return getOrFail("cdac.jdbcschemaraw.paramname");
    }

    public String getJdbcSchemaRawParamValue() {
        return getOrFail("cdac.jdbcschemaraw.paramvalue");
    }

    public String getJdbcSchemaMergedParamName() {
        return getOrFail("cdac.jdbcschemamerged.paramname");
    }

    public String getJdbcSchemaMergedParamValue() {
        return getOrFail("cdac.jdbcschemamerged.paramvalue");
    }

    public String getJdbcUrlParamName() {
        return getOrFail("cdac.jdbcurl.paramname");
    }

    public String getJdbcUrlParamValue() {
        return getOrFail("cdac.jdbcurl.paramvalue");
    }

    public String getStoredProcSchemaParamName() {
        return getOrFail("cdac.storedprocschema.paramname");
    }

    public String getStoredProcSchemaParamValue() {
        return getOrFail("cdac.storedprocschema.paramvalue");
    }

    public String getStoredProcMergedViewParamName() {
        return getOrFail("cdac.storedprocmergedview.paramname");
    }

    public String getStoredProcMergedViewParamValue() {
        return getOrFail("cdac.storedprocmergedview.paramvalue");
    }

    public String getStoredProcSQLBucketParamName() {
        return getOrFail("cdac.storedprocsqlbucket.paramname");
    }

    public String getStoredProcSQLBucketParamValue() {
        return getOrFail("recon.lambda.bucket");
    }

    public String getStoredProcSQLS3KeyPathParamName() {
        return getOrFail("cdac.storedprocsqls3Key.paramname");
    }

    public String getStoredProcSQLS3KeyParamValue() {
        return getOrFail("cdac.storedprocsqls3Key.paramvalue");
    }

    public String getExcludeTableParamName() {
        return getOrFail("cdac.excludetable.paramname");
    }

    public String getExcludeTableParamValue() {
        return getOrFail("cdac.excludetable.paramvalue");
    }

    public String getCDAS3SecretParamName() {
        return getOrFail("cdac.cdas3secret.paramname");
    }

    public String getCDAS3SecretParamValue() {
        return getOrFail("cdac.cdas3secret.paramvalue");
    }

    public String getRDSSecretParamName() {
        return getOrFail("cdac.rdssecret.paramname");
    }

    public String getRDSSecretParamValue() {
        return getOrFail("cdac.rdssecret.paramvalue");
    }

    public String getEmailForNotifications() {
        return getOrFail("cdac.email.notifications");
    }


    public String getSecretsEndpointParamName() {
        return getOrFail("cdac.secretsendpoint.paramname");
    }

    public String getSecretsEndpointParamValue() {
        return getOrFail("cdac.secretsendpoint.paramvalue");
    }

    public String getBatchSizeParamName() {
        return getOrFail("cdac.batchsize.paramname");
    }
    public String getBatchSizeParamValue() {
        return getOrFail("cdac.batchsize.paramvalue");
    }

    public String getBatchManifestHandler() {
        return getOrFail("batchmanifestentity.lambda.handler");
    }

    public String getCDAEntityChangeHandler() {
        return getOrFail("cda.entitychangecheck.lambda.handler");
    }

    public String getEmrLogsS3ParamName() {
        return getOrFail("cdac.emrlog.paramname");
    }

    public String getEmrLogsS3ParamValue() {
        return getOrFail("cdac.emrlog.paramvalue");
    }

    public String getReconLambdaS3ParamName() {
        return getOrFail("cdac.reconlambda.paramname");
    }

    public String getReconLambdaS3ParamValue() {
        return getOrFail("cdac.reconlambda.paramvalue");
    }


}