package cdacRecon;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.json.simple.JSONObject;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.lambda.powertools.parameters.ParamManager;
import software.amazon.lambda.powertools.parameters.SSMProvider;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Lambda that initiates RDS - SQL Server back up. As of writing this lambda, RDS SQL Server back up is only possible
 * by calling an internal stored procedure. The Stored Procedure returns a taskId that can be used to query the status
 * of the dbBackUp.
 * It is assumed that the user of this lambda has completed the necessary pre-requisites mentioned here :
 * https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/SQLServer.Procedural.Importing.html#SQLServer.Procedural.Importing.Native.Using
 * This lambda returns a json object that contains the taskId of the DBBackup. This return object will normally be used
 * in StepFunctions to check the completion status. The lambda does not take any inputs.
 * */
public class DBBackUpHandler implements RequestHandler<String, String> {

    //Initialize configuration class to get properties
    Configuration conf = new Configuration();

    //Product is the Guidewire application for which DBBackUp has to be initiated. This could be one of claimcenter,
    // policycenter, billingcenter and contactcenter.
    String product = conf.getLambdaParamPrefix()+"/entities";

    //Initialize ssmClient to retrieve the required parameters from Systems Manager.
    SsmClient client = SsmClient.builder().region(Region.US_EAST_1).build();
    Gson gson = new GsonBuilder().setPrettyPrinting().create();

    @Override
    public String handleRequest(String s, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("CONTEXT: " + gson.toJson(context));
        SSMProvider ssmProvider = ParamManager.getSsmProvider(client);
        String taskId = "";
        Map<String, String> map = new HashMap<String, String>();
        JSONObject obj = new JSONObject();

        try {
            //RDSQuery is a supporting class that interacts with Amazon RDS
            RDSQuery rdsQuery = new RDSQuery();
            //Call RDSQuery.startDBBackUp function to start backup
            taskId = rdsQuery.startDBBackUp(logger, client, ssmProvider.get(product));
            //adding the taskId to map to return in Lambda response
            map.put("taskId", taskId);
            obj.putAll(map);
        } catch (SQLException e) {
            logger.log("SQLException occurred in BackUpHandler.handleRequest :"+e.getMessage());
            throw new RuntimeException(e);
        }
        return obj.toJSONString();
    }
}
