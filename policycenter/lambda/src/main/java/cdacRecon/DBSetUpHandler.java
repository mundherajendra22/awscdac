package cdacRecon;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.lambda.powertools.parameters.ParamManager;
import software.amazon.lambda.powertools.parameters.SSMProvider;

import java.sql.SQLException;

/**
 * Lambda that creates RDS - SQL Server. Using this Lambda is optional. The lambda is used when customer does not want
 * to provide access to RDS SQL Server directly and only through JDBC.
 * This is a standalone lambda that creates database for Amazon RDS SQL Server. It can be executed mannually through
 * AdminConsole or any other means that the customer provides.
 * The lambda does not take any inputs
 */
public class DBSetUpHandler implements RequestHandler<String, String> {

    //Initialize configuration class to get properties
    Configuration conf = new Configuration();

    //Product is the Guidewire application for which Database has to be created. This could be one of claimcenter,
    // policycenter, billingcenter and contactcenter.
    String product = conf.getLambdaParamPrefix()+"/entities";

    //Initialize ssmClient to retrieve the required parameters from Systems Manager.
    SsmClient client = SsmClient.builder().region(Region.US_EAST_1).build();

    @Override
    public String handleRequest(String s, Context context) {
        LambdaLogger logger = context.getLogger();
        SSMProvider ssmProvider = ParamManager.getSsmProvider(client);
        Boolean result = false;

        try {
            //RDSQuery is a supporting class that interacts with Amazon RDS
            RDSQuery rdsQuery = new RDSQuery();

            //Call RDSQuery.createDatabase function to create database
            result = rdsQuery.createDatabase(logger, client, product);
            logger.log("Database created "+result);
        } catch (SQLException e) {
            logger.log("SQLException occurred in BackUpHandler.handleRequest :"+e.getMessage());
            throw new RuntimeException(e);
        }
        return "Database created "+result;
    }
}
