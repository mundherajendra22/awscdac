package cdacRecon;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.lambda.powertools.parameters.ParamManager;
import software.amazon.lambda.powertools.parameters.SSMProvider;

// create a lambda handler to connect to RDS and execute stored procedure
public class RDSStoredProcedureHandler implements RequestHandler<String, String> {
    //Initialize configuration class to get properties
    Configuration conf = new Configuration();
    String lambdaParam = conf.getLambdaParamPrefix();
    //Initialize ssmClient to retrieve the required parameters from Systems Manager.
    SsmClient client = SsmClient.builder().region(Region.US_EAST_1).build();

    @Override
    public String handleRequest(String s, Context context) {
        LambdaLogger logger = context.getLogger();
        SSMProvider ssmProvider = ParamManager.getSsmProvider(client);
        //RDSStoredProc is a supporting class that interacts with Amazon RDS
        //Call RDSStoredProc.executeMergedView function to start Stored Procedure execution
        new RDSStoredProc().executeMergedView(ssmProvider, logger);
        logger.log("Starting the Stored Proc Exceution");
        return "200 - OK";

    }
}