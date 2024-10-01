package cdacRecon;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.json.simple.JSONObject;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.lambda.powertools.parameters.ParamManager;
import software.amazon.lambda.powertools.parameters.SSMProvider;

public class DBTransLog implements RequestHandler<String, JSONObject> {
    @Override
    public JSONObject handleRequest(String s, Context context) {
        LambdaLogger logger = context.getLogger();
        SsmClient client = SsmClient.builder().region(Region.US_EAST_1).build();
        SSMProvider ssmProvider = ParamManager.getSsmProvider(client);
        RDSQuery rdsQuery = new RDSQuery();



        return null;
    }
}
