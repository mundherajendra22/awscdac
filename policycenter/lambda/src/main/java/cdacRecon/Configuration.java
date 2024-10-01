package cdacRecon;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.lambda.powertools.parameters.ParamManager;
import software.amazon.lambda.powertools.parameters.SSMProvider;

/**
 * A bean class that reads properties from AWS Systems Manager -> Parameter Store for the respective environment
 * This class will be used in Lambdas to load parameters
 * Author: Jay Mehta
 */


public class Configuration {

    //Initialize ssmClient to retrieve the required parameters from Systems Manager.
    SsmClient client = SsmClient.builder().region(Region.US_EAST_1).build();
    SSMProvider ssmProvider = ParamManager.getSsmProvider(client);


    public String getLambdaParamPrefix() {
        return ssmProvider.get("/citizens/cda/lambda/prefix/gwpc-dev");
    }
    
    public String getManifestKey() {
        return ssmProvider.get("/citizens/cda/manifest/key/gwpc-dev");
    }
    
    public Region getCustomerRegion() {
        return Region.US_EAST_1;
    }

    public String getJdbcSchemaRaw() {
        return ssmProvider.get(getLambdaParamPrefix() + "/jdbcSchemaRaw");
    }

    public String getJdbcSchemaMerged() {
        return ssmProvider.get(getLambdaParamPrefix() + "/jdbcSchemaMerged");
    }

    public String getStoredProcSchema() {
        return ssmProvider.get(getLambdaParamPrefix() + "/storedProcSchema");
    }

    public String getStoredProcMergedView() {
        return ssmProvider.get(getLambdaParamPrefix() + "/storedProcMergedView");
    }

    public String getStoredProcBucket() {
        return ssmProvider.get(getLambdaParamPrefix() + "/storedProcSQLBucket");
    }

    public String getStoredProcKey() {
        return ssmProvider.get(getLambdaParamPrefix() + "/storedProcSQLS3Key");
    }

    public String getBatchSize() {
        return ssmProvider.get(getLambdaParamPrefix() + "/batchSize");
    }

    public String getAuditBucket() {
        return ssmProvider.get(getLambdaParamPrefix() + "/auditreport");
    }

}
