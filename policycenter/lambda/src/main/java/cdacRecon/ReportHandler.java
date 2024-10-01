package cdacRecon;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.lambda.powertools.parameters.ParamManager;
import software.amazon.lambda.powertools.parameters.SSMProvider;


/**
 * Lambda that reads a DynamoDB table and writes its contents in a csv file in a s3 bucket.
 * This is a standalone lambda and can be executed mannually through AdminConsole or any other means that the
 * customer provides. It is used in StepFunction to create Audit&Controls report. It is recommended that this lambda be
 * used in liaison with cdacRecon.Handler lambda. The lambda does not take any inputs.
 */
public class ReportHandler implements RequestHandler<String, String> {

    //Initialize configuration class to get properties
    Configuration conf = new Configuration();

    //Guidewire application for which the AWS CDAC has to be launched. This could be one of claimcenter,
    //policycenter, billingcenter and contactcenter - comma separated values in AWS Systems Manager
    String entities = conf.getLambdaParamPrefix()+"/entities";

    //Initialize ssmClient to retrieve the required parameters from Systems Manager.
    SsmClient client = SsmClient.builder().region(Region.US_EAST_1).build();


    @Override
    public String handleRequest(String s, Context context) {
        LambdaLogger logger = context.getLogger();
        SSMProvider ssmProvider = ParamManager.getSsmProvider(client);
        String tables = ssmProvider.get(entities);
        PersistEntity persistEntity = new PersistEntity();
        String[] tableArray = tables.split(",");

        //write all items from dynamodb table to s3 bucket
        persistEntity.writeAllItems(logger, tableArray[0], ssmProvider.get(conf.getLambdaParamPrefix()+"/auditreport"));
        logger.log("Done writing recon report for "+tableArray[0]);
        logger.log("Done writing all files to S3");
        return "Completed writing recon reports";
    }
}
