package cdacRecon;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.lambda.powertools.parameters.ParamManager;
import software.amazon.lambda.powertools.parameters.SSMProvider;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Lambda that check RDS - SQL Server back up status. As of writing this lambda, RDS SQL Server back up is only possible
 * by calling an internal stored procedure. The Stored Procedure returns lifecycle status fpr a taskId
 * It is assumed that the user of this lambda has completed the necessary pre-requisites mentioned here :
 * https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/SQLServer.Procedural.Importing.html#SQLServer.Procedural.Importing.Native.Using
 * This lambda returns a json object that contains the status of the DBBackup. This return object will normally be used
 * in StepFunctions to check the completion status. The lambda takes taskId as input.
 */
public class DBBackUpStatusHandler implements RequestHandler<Map<String, String>, JSONObject> {

    //Initialize configuration class to get properties
    Configuration conf = new Configuration();

    //Product is the Guidewire application for which DBBackUp status has to be checked. This could be one of claimcenter,
    // policycenter, billingcenter and contactcenter.
    String product = conf.getLambdaParamPrefix()+"/entities";

    //Initialize ssmClient to retrieve the required parameters from Systems Manager.
    SsmClient client = SsmClient.builder().region(Region.US_EAST_1).build();

    Gson gson = new GsonBuilder().setPrettyPrinting().create();

    @Override
    public JSONObject handleRequest(Map<String, String> s, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("CONTEXT: " + gson.toJson(context));
        SSMProvider ssmProvider = ParamManager.getSsmProvider(client);
        String input = gson.toJson(s);
        logger.log("Input is: " + input);
        String lifecycle = "";
        String taskId="";

        Map<String, String> map = new HashMap<String, String>();
        JSONObject obj = null;

        try {
            //RDSQuery is a supporting class that interacts with Amazon RDS
            RDSQuery rdsQuery = new RDSQuery();

            //Parse input json to get taskId
            obj = (JSONObject) new JSONParser().parse(input);
            taskId = obj.get("taskId").toString();
            //Call RDSQuery.dbBackUpStatus function to get status of taskId
            lifecycle = rdsQuery.dBBackUpStatus(logger, client, ssmProvider.get(product), taskId);
            map.put("lifecycle", lifecycle);
            map.put("taskId", taskId);
            obj.putAll(map);
        } catch (SQLException e) {
            logger.log("SQLException occurred in DBBackUpStatusHandler.handleRequest :"+e.getMessage());
            throw new RuntimeException(e);
        } catch (ParseException e) {
            logger.log("ParseException occurred in DBBackUpStatusHandler.handleRequest :"+e.getMessage());
            throw new RuntimeException(e);
        }
        return obj;
    }
}