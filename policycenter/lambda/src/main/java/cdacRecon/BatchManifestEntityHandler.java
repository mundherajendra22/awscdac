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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonArrayBuilder;


/**
 * Lambda that creates batch manifest entities. The output will be used to get loaded entities statistics.
 */
public class BatchManifestEntityHandler implements RequestHandler<String, String> {

    //Initialize configuration class to get properties
    Configuration conf = new Configuration();

    //Guidewire application for which the AWS CDAC has to be launched. This could be one of claimcenter,
    //policycenter, billingcenter and contactcenter - comma separated values in AWS Systems Manager
    String entities = conf.getLambdaParamPrefix()+"/entities";

    //Initialize ssmClient to retrieve the required parameters from Systems Manager.
    SsmClient client = SsmClient.builder().region(Region.US_EAST_1).build();
    Gson gson = new GsonBuilder().setPrettyPrinting().create();

    JsonArrayBuilder entityArrObject = Json.createArrayBuilder();

    @Override
    public String handleRequest(String s, Context context) {
        LambdaLogger logger = context.getLogger();
        SSMProvider ssmProvider = ParamManager.getSsmProvider(client);

        String tables = ssmProvider.get(entities);
        String[] tableArray = tables.split(",");

        String bucketName = "";
        String manifestKey= conf.getManifestKey();

        Integer batchSize = Integer.parseInt(conf.getBatchSize());

        try {

            logger.log("First Parameter :"+conf.getLambdaParamPrefix()+"/bucketname");
            //get GW CDA S3 Bucket name
            bucketName = ssmProvider.get(conf.getLambdaParamPrefix()+"/bucketname");

            //list of tables to be excluded when reading manifest file
            String tablesToExclude = ssmProvider.get(conf.getLambdaParamPrefix()+"/tablesToExclude");

            //get manifest map after parsing the manifest file
            Map<String, ManifestReader.ManifestEntry> manifest = ManifestReader.processManifest(bucketName, manifestKey, logger, client, tableArray[0]);


            // Create array of entities
            JsonArrayBuilder entityArr = Json.createArrayBuilder();

            Integer i = 0, totalEntities = 0;

            //Loop over each key of the manifest file
            for (Map.Entry<String, ManifestReader.ManifestEntry> pair : manifest.entrySet()) {
                String entityName = pair.getKey();
                if(!tablesToExclude.contains(entityName)){
                    if (i<batchSize){
                        entityArr.add (Json.createObjectBuilder().add(pair.getKey(), Json.createObjectBuilder()
                                .add("lastSuccessfulWriteTimestamp", pair.getValue().getLastSuccessfulWriteTimestamp())
                                .add("totalProcessedRecordsCount", pair.getValue().getTotalProcessedRecordsCount())
                                .build()
                        ).build());

                        i++;
                    }
                    else{
                        entityArr.add(Json.createObjectBuilder().add(pair.getKey(), Json.createObjectBuilder()
                                        .add("lastSuccessfulWriteTimestamp", pair.getValue().getLastSuccessfulWriteTimestamp())
                                        .add("totalProcessedRecordsCount", pair.getValue().getTotalProcessedRecordsCount())
                                        .build()));

                        entityArrObject.add(Json.createObjectBuilder().add("entities", entityArr.build()).build());
                        i=0;
                    }
                    totalEntities++;
                } else {
                    logger.log("Excluding Entity "+entityName+" in Recon for "+tableArray[0]);
                }
            }

            if (i!=0 || i+1!=batchSize){
                entityArrObject.add(Json.createObjectBuilder().add("entities", entityArr.build()).build());
            }

            // Placeholder, if Lambda output exceed 256 KB. Current output size for 1.1k tables is 120KB
            //Save output to S3
            //if (totalEntities!=0){

                //build the s3 client using creds and vpcendpoint
            //    S3Client s3Client=S3Client.builder().region(Region.US_EAST_1)
                        //.credentialsProvider(staticCredentialsProvider)
                        //.endpointOverride(uri)
            //            .build();

            //    PutObjectRequest objectRequest = PutObjectRequest.builder()
            //            .bucket("bucket name")
            //            .key("key name")
            //            .build();

            //    s3Client.putObject(objectRequest, RequestBody.fromString(entityArrObject.build().toString()));

            //}

        } catch (IOException e) {
        throw new RuntimeException(e);
        }
        //catch (ClassNotFoundException e) {
        //    throw new RuntimeException(e);
        //}

        return entityArrObject.build().toString();
    }
}
