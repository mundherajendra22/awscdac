package cdacRecon;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.json.simple.JSONObject;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.lambda.powertools.parameters.ParamManager;
import software.amazon.lambda.powertools.parameters.SSMProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Lambda that checks launch conditions for AWS CDAC. As of writing this lambda, there is no feature in Guidewire CDA
 * that emits events. So, it is the customers prerogative to check for conditions when to launch AWS CDAC.
 * This lambda compares the LastSuccessfulWriteTimestamp in manifest json and DynamoDB table, which contains the time
 * from lastRun, and returns a string response START/STOP which is used in StepFunctions to run the connector.
 * The lambda does not take any inputs.
 * */
public class CDACheckHandlerChange implements RequestHandler<Map<String,String>, String> {

    //Initialize configuration class to get properties
    Configuration conf = new Configuration();

    //Guidewire application for which the AWS CDAC has to be launched. This could be one of claimcenter,
    // policycenter, billingcenter and contactcenter.
    String entities = conf.getLambdaParamPrefix()+"/entities";
    String entitiesChanges = conf.getLambdaParamPrefix()+"/entityChangeDynamoDBName";

    //Initialize ssmClient to retrieve the required parameters from Systems Manager.
    SsmClient client = SsmClient.builder().region(Region.US_EAST_1).build();

    @Override
    public String handleRequest(Map<String, String> s, Context context) {
        LambdaLogger logger = context.getLogger();
        SSMProvider ssmProvider = ParamManager.getSsmProvider(client);

        String tables = ssmProvider.get(entities);
        String[] tableArray = tables.split(",");

        String tablesChange = ssmProvider.get(entitiesChanges);
        String[] tableChangeArray = tablesChange.split(",");

        String bucketName = "";
        //manifest file name in GW's CDA S3 bucket
        String manifestKey= conf.getManifestKey();
        PersistEntity pEnt = new PersistEntity();
        PersistEntityChange pEntChange = new PersistEntityChange();

        //GW's CDA S3 bucket name
        bucketName = ssmProvider.get(conf.getLambdaParamPrefix()+"/bucketname");
        logger.log("GW input bucket name: "+bucketName);
        logger.log("GW manifest key: "+manifestKey);
        Map<String, ManifestReader.ManifestEntry> manifest = null;
        Map<String, String> map = new HashMap<String, String>();


        //Step Function execution Id in the arn:aws:states:<region>:<account id>:execution:<step function name>:<execution id> format
        int index = s.get("stepFunctionExecutionId").lastIndexOf(':');
        String stepFunctionExecutionId = s.get("stepFunctionExecutionId").substring(index +1);

        //List of tables for which the condition check should be excluded
        String tablesToExclude = ssmProvider.get(conf.getLambdaParamPrefix()+"/tablesToExclude");
        try {
            manifest = ManifestReader.processManifest(bucketName, manifestKey, logger, client, tableArray[0]);
            for (Map.Entry<String, ManifestReader.ManifestEntry> pair : manifest.entrySet()) {
                if(!tablesToExclude.contains(pair.getKey())){
                    //Check if the key - which is tableName exists in DynamoDB from last run.
                    PersistEntity.Entity lastEntity = pEnt.getLastRunItem(pair.getKey(),logger, tableArray[0]);
                    String lastTS = pair.getValue().getLastSuccessfulWriteTimestamp();
                    String entityName = pair.getKey();

                    //Compare the field lastSuccessfulWriteTimestamp for parquet files between manifest and DynamoDB.
                    if(lastEntity == null || (lastEntity!=null && !lastTS.equalsIgnoreCase(lastEntity.getLastSuccessfulWritePq()))){
                        logger.log("Starting CDAC since lastSuccessfulWriteTimestamp does not match for :"+pair.getKey());
                        pEntChange.putRecord(tableChangeArray[0],entityName, stepFunctionExecutionId, s.get("stepFunctionStartExecution"),  logger, client);
                    }
                } else {
                    logger.log("Table exlcuded from CDAC check "+pair.getKey());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e){
            throw new RuntimeException(e);
        }

        return "200 - OK";
    }
}