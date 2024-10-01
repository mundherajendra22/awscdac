package cdacRecon;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.lambda.powertools.parameters.ParamManager;
import software.amazon.lambda.powertools.parameters.SSMProvider;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Helper class to read Guidewire CDA Manifest file
 */
public class ManifestReader {

    //Initialize configuration class to get properties
    static Configuration conf = new Configuration();

    //Initialize GW CDA s3 credentials secret name
    static final String gwCDASecretName = "cdac/gwcda";
    static Region region = Region.US_EAST_1;//GWCP is currently available in us-east-1 for North American customers

    /**
     * Static function that will be used to read the manifest file
     * @param bucketName
     * @param manifestKey
     * @param logger
     * @param client
     * @param productName
     * @return a map containing the manifest file contents
     * @throws IOException
     */
    public static Map<String, ManifestEntry> processManifest(String bucketName, String manifestKey, LambdaLogger logger, SsmClient client, String productName) throws IOException {
        return parseManifestJson(getManifestJson(bucketName, manifestKey, logger, client, productName), logger);
    }

    /**
     * Function that parses the manifest and loads its contents into a map
     * @param manifestJson
     * @param logger
     * @return a map containing the manifest file contents
     * @throws IOException
     */
    private static Map<String, ManifestEntry> parseManifestJson(String manifestJson, LambdaLogger logger) throws IOException {
        //create a typereference for map that contains the manifest file contents
        TypeReference<Map<String,ManifestEntry>> typeRef = new TypeReference<Map<String,ManifestEntry>>(){};
        logger.log("Object :"+manifestJson);
        //parse the manifest file based on the typereference
        Map<String, ManifestEntry> entry = new ObjectMapper().readValue(manifestJson, typeRef);
        logger.log("Successfully parsed manifest JSON file");
        return entry;
    }

    /**
     * Function that gets the manifest from s3 bucket and returns it as a string
     * @param bucketName
     * @param manifestKey
     * @param logger
     * @param ssmClient
     * @param productName
     * @return
     */
    private static String getManifestJson(String bucketName, String manifestKey, LambdaLogger logger, SsmClient ssmClient, String productName) {
        SSMProvider ssmProvider = ParamManager.getSsmProvider(ssmClient);
        //Get GW CDA s3 bucket credentials from AWS Secrets Manager - this is not needed for cross account access
        /*String[] parameters = getCDAS3Password(ssmProvider);

        //Credentials provider to access GW CDA S3 bucket
        StaticCredentialsProvider staticCredentialsProvider =  StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                        parameters[0],
                        parameters[1]));

        //SSM getParameter request for s3 VPCEndpoint URL
        String endpointURL = ssmProvider.get(conf.getLambdaParamPrefix()+"/cdas3endpointURL");

        logger.log("Endpoint URL is :"+endpointURL);
        URI uri = null;
        */

        String manifest = "";
        try {
            //uri = new URI(endpointURL);

            //build the s3 client using creds and vpcendpoint
            S3Client s3Client=  S3Client.builder().region(Region.US_EAST_1)
                    //.credentialsProvider(staticCredentialsProvider)
                    //.endpointOverride(uri)
                    .build();

            //build the getObjectRequest using bucket name and object key
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(manifestKey)
                    .build();

            //Api call to get the object contents in a string
            manifest =  s3Client.getObjectAsBytes(getObjectRequest).asUtf8String();
            logger.log("Read manifest file from bucket :"+bucketName+" with key :"+manifestKey);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return manifest;
    }


    /**
     * Bean class that represents the manifest file format
     */
    public static class ManifestEntry {
        private String lastSuccessfulWriteTimestamp;
        private Integer totalProcessedRecordsCount;
        private String dataFilesPath;
        private Map<String, String> schemaHistory;

        /**
         *
         * @return
         */
        public String getLastSuccessfulWriteTimestamp() {
            return lastSuccessfulWriteTimestamp;
        }

        /**
         *
         * @param lastSuccessfulWriteTimestamp
         */
        public void setLastSuccessfulWriteTimestamp(String lastSuccessfulWriteTimestamp) {
            this.lastSuccessfulWriteTimestamp = lastSuccessfulWriteTimestamp;
        }

        /**
         *
         * @return
         */
        public Integer getTotalProcessedRecordsCount() {
            return totalProcessedRecordsCount;
        }

        /**
         *
         * @param totalProcessedRecordsCount
         */
        public void setTotalProcessedRecordsCount(Integer totalProcessedRecordsCount) {
            this.totalProcessedRecordsCount = totalProcessedRecordsCount;
        }

        /**
         *
         * @return
         */
        public String getDataFilesPath() {
            return dataFilesPath;
        }

        /**
         *
         * @param dataFilesPath
         */
        public void setDataFilesPath(String dataFilesPath) {
            this.dataFilesPath = dataFilesPath;
        }

        /**
         *
         * @return
         */
        public Map<String, String> getSchemaHistory() {
            return schemaHistory;
        }

        /**
         *
         * @param schemaHistory
         */
        public void setSchemaHistory(Map<String, String> schemaHistory) {
            this.schemaHistory = schemaHistory;
        }
    }

    private static String[] getCDAS3Password(SSMProvider ssmProvider) {
        try{
            SecretsManagerClient client = SecretsManagerClient.builder().region(region)
                    .endpointOverride(new URI("https://vpce-080e8cb6e216af69c-rpcq26nf.secretsmanager.us-east-1.vpce.amazonaws.com"))
                    .build();
            GetSecretValueRequest request = GetSecretValueRequest.builder().secretId(ssmProvider.getValue(conf.getLambdaParamPrefix()+"/GWCDASecretName")).build();
            GetSecretValueResponse response;
            response = client.getSecretValue(request);
            JSONObject jsonObject = (JSONObject) JSONValue.parse(response.secretString());
            return new String[]{(String)jsonObject.get("username"),(String)jsonObject.get("password")};
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}