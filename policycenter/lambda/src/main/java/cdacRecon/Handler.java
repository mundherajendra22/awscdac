package cdacRecon;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.lambda.powertools.parameters.ParamManager;
import software.amazon.lambda.powertools.parameters.SSMProvider;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.List;

/**
 * Lambda that parses Guidewire CDA manifest file and upserts its contents in a DynamoDB table.
 * This is a standalone lambda that updates a dynamodbtable. The dynamodb table is created as a pre-requisite.
 * It can be executed mannually through AdminConsole or any other means that the customer provides.
 * It is used in StepFunction to create Audit&Controls report. The lambda does not take any inputs.
 */

public class Handler implements RequestHandler<Map<String, List<Map<String, ManifestReader.ManifestEntry>>>, String> {

    //Initialize configuration class to get properties
    Configuration conf = new Configuration();

    //Guidewire application for which the AWS CDAC has to be launched. This could be one of claimcenter,
    //policycenter, billingcenter and contactcenter - comma separated values in AWS Systems Manager
    String entities = conf.getLambdaParamPrefix()+"/entities";

    //Initialize ssmClient to retrieve the required parameters from Systems Manag
    SsmClient client = SsmClient.builder().region(Region.US_EAST_1).build();
    Gson gson = new GsonBuilder().setPrettyPrinting().create();

    @Override
    public String handleRequest(Map<String, List<Map<String, ManifestReader.ManifestEntry>>> s, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("CONTEXT: " + gson.toJson(context));
        String input = gson.toJson(s);
        logger.log("INPUT: " + input);
        SSMProvider ssmProvider = ParamManager.getSsmProvider(client);

        String tables = ssmProvider.get(entities);
        String[] tableArray = tables.split(",");
        String bucketName = "";
        String manifestKey= conf.getManifestKey();
        PersistEntity pEnt = new PersistEntity();
        String lambdaParam = conf.getLambdaParamPrefix();
        //Retrieve JDBC parameters
        String jdbcURL =lambdaParam+"/jdbcUrl";
        String creds[] = new RDSQuery().getRDSPassword(ssmProvider);
        String jdbcUsername = creds[0];
        String jdbcPassword = creds[1];
        logger.log("JDBC Parameters :"+jdbcURL);
        String jdbcUrl = ssmProvider.get(jdbcURL);
        Connection con = null;

        try {
            logger.log("First Parameter :"+conf.getLambdaParamPrefix()+"/bucketname");
            //get GW CDA S3 Bucket name
            bucketName = ssmProvider.get(conf.getLambdaParamPrefix()+"/bucketname");

            //JDBC Connection
            Class.forName("org.postgresql.Driver");
            con = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
            con.setAutoCommit(false);

            //list of tables to be excluded when reading manifest file
            String tablesToExclude = ssmProvider.get(conf.getLambdaParamPrefix()+"/tablesToExclude");

            //Loop over each key of the manifest file
            //for (Map.Entry<String, List<ManifestReader.ManifestEntry>> iteration : s.entrySet()) {
            //    String key = iteration.getKey();
            //    List<String> entityNameList = iteration.getValue();

              //  for (String entityName : entityNameList) {
                //    if( !tablesToExclude.contains(entityName)){
                  //      int currentRun = pair.getValue().getTotalProcessedRecordsCount();
                    //    String lastSuccessfulWritePq = pair.getValue().getLastSuccessfulWriteTimestamp();
                        //write record to DynamoDB table
                        //pEnt.putRecord(con, tableArray[0],entityName, lastSuccessfulWritePq, currentRun, logger, client);
                   // } else {
                    //    logger.log("Excluding Entity "+entityName+" in Recon for "+tableArray[0]);
                    //}
                //}
            //}

            //get manifest map after parsing the manifest file
            //Map<String, ManifestReader.ManifestEntry> manifest = ManifestReader.processManifest(bucketName, manifestKey, logger, client, tableArray[0]);

            for (Map.Entry<String, List<Map<String, ManifestReader.ManifestEntry>>> batchRun : s.entrySet()) {

                for (Map<String, ManifestReader.ManifestEntry> entity : batchRun.getValue()) {

                    for (Map.Entry<String, ManifestReader.ManifestEntry> pair : entity.entrySet()) {
                        String entityName = pair.getKey();
                        if (!tablesToExclude.contains(entityName)) {
                            int currentRun = pair.getValue().getTotalProcessedRecordsCount();
                            String lastSuccessfulWritePq = pair.getValue().getLastSuccessfulWriteTimestamp();
                            //write record to DynamoDB table
                            pEnt.putRecord(con, tableArray[0],entityName, lastSuccessfulWritePq, currentRun, logger, client);
                        } else {
                            logger.log("Excluding Entity " + entityName + " in Recon for " + tableArray[0]);
                        }
                    }

                }

            }
        } //catch (IOException e) {
            //throw new RuntimeException(e);
        //}
        catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if(con!=null && !con.isClosed()){
                    con.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        logger.log("Completed Audit & Controls for "+tableArray[0]);

        return "200 - OK";
    }
}


//{
//  "table_list1": "$$.Map.Item.Value"
//}