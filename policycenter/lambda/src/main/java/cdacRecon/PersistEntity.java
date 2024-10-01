package cdacRecon;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.opencsv.CSVWriter;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.model.GetItemEnhancedRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.ssm.SsmClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Helper class that interacts with DynamoDB
 */
public class PersistEntity {
    Region region = Region.US_EAST_1;
    DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    // Create a DynamoDbClient object
    DynamoDbClient ddb = DynamoDbClient.builder()
            .region(region)
            .build();
    // Create a DynamoDbEnhancedClient and use the DynamoDbClient object
    DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
            .dynamoDbClient(ddb)
            .build();

    String dynamoDbKey = "name";

    /**
     * Puts an item into a DynamoDB table
     * @param dynamoTableName
     * @param entityName
     * @param lastSuccessfulWritePq
     * @param currentRun
     * @param logger
     * @param client
     */
    public void putRecord(Connection con, String dynamoTableName, String entityName, String lastSuccessfulWritePq, int currentRun, LambdaLogger logger, SsmClient client) {
        try {
            // Create a DynamoDbTable object
            DynamoDbTable<Entity> entityDynamoDbTable = enhancedClient.table(dynamoTableName, TableSchema.fromBean(Entity.class));
            Key key = Key.builder()
                    .partitionValue(entityName)
                    .build();
            Entity existingEntity = entityDynamoDbTable.getItem(r -> r.key(key));

            logger.log("PersistentEntity.putRecord - record for entity "+entityName);
            if(existingEntity != null){
                //Update existing entity data
                int lastVal = Integer.parseInt(existingEntity.getCurrentRun());
                logger.log("PersistentEntity.putRecord - updating record for entity "+existingEntity.name+
                        " with previous parquetCount :"+lastVal+ " and currentCount :"+currentRun);
                RDSQuery rdsObj = new RDSQuery();
                existingEntity.setLastSuccessfulWritePq(lastSuccessfulWritePq);
                existingEntity.setCurrentRun(String.valueOf(currentRun));
                //Get RDS table count for corresponding entity name
                existingEntity.setTableCount(String.valueOf( rdsObj.getTableCount(con, entityName, logger, client, dynamoTableName)));
                existingEntity.setRunDate(LocalDateTime.now().atOffset(ZoneOffset.UTC).format(format));
                entityDynamoDbTable.updateItem(r -> r.item(existingEntity));
                logger.log("PersistentEntity.putRecord - completed updating record for entity "+entityName);
            } else {
                // Populate the table object for a new entity
                RDSQuery rdsObj = new RDSQuery();
                Entity entityRecord = new Entity();
                entityRecord.setName(entityName);
                entityRecord.setLastSuccessfulWritePq(lastSuccessfulWritePq);
                entityRecord.setCurrentRun(String.valueOf(currentRun));
                entityRecord.setTableCount(String.valueOf(rdsObj.getTableCount(con, entityName, logger, client, dynamoTableName)));
                entityRecord.setRunDate(LocalDateTime.now().atOffset(ZoneOffset.UTC).format(format));
                // Put the entity data into a DynamoDB table
                entityDynamoDbTable.putItem(entityRecord);
                logger.log("PersistentEntity.putRecord - completed inserting record for entity "+entityName);
            }
        } catch (DynamoDbException e) {
            logger.log("Exception occurred while upserting dynamoDB :"+e.getMessage());
            System.exit(1);
        } catch (SQLException e) {
            logger.log("SQLException occurred while upserting dynamoDB :"+e.getMessage());
            throw new RuntimeException(e);
        }
        logger.log("PersistentEntity.putRecord - done "+entityName);
    }

    /**
     * Returns Item based on a key which is entityName
     * @param entityName
     * @param logger
     * @param dynamoTableName
     * @return
     */
    public Entity getLastRunItem(String entityName, LambdaLogger logger, String dynamoTableName) {

        Entity result = null;

        try {
            //Get dynammoDB table modeled as Entity object
            DynamoDbTable<Entity> table = enhancedClient.table(dynamoTableName, TableSchema.fromBean(Entity.class));

            //Get Key based on entityName
            Key key = Key.builder()
                    .partitionValue(entityName)
                    .build();

            // Get the item by using the key.
            result = (Entity) table.getItem(
                    (GetItemEnhancedRequest.Builder requestBuilder) -> requestBuilder.key(key));

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        //returning current run from entity recon table so that it can be updated as lastrun in the next execution
        if(result!= null && result.getLastSuccessfulWritePq()!= null && !result.getLastSuccessfulWritePq().isEmpty()){
            logger.log("******* The Entity Name is " + result.getName());
            return result;
        }

        return null;
    }

    /**
     * Writes all Items in a CSV file from DynamoDB table to a S3 Bucket, uses OpenCSV
     * @param logger
     * @param dynamoTableName
     * @param bucketName
     */
    public void writeAllItems(LambdaLogger logger, String dynamoTableName, String bucketName) {
        try{
            //Initialize stream and writer objects
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            OutputStreamWriter streamWriter = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
            CSVWriter csvWriter = new CSVWriter(streamWriter, ',', Character.MIN_VALUE, '"', System.lineSeparator());

            //Get dynamoDB table modeled as Entity object
            DynamoDbTable<Entity> custTable = enhancedClient.table(dynamoTableName, TableSchema.fromBean(Entity.class));

            //Scan all items from DynamoDB table
            Iterator<Entity> results = custTable.scan().items().iterator();
            List<String[]> records = new ArrayList<String[]>();

            //Add column headers to csvWriter
            records.add(new String[]{"EntityName", "ParquetCount", "DBCount", "RunDate"});
            while (results.hasNext()) {
                Entity rec = (Entity) results.next();

                //Retrieve each record in csvWriter
                records.add(new String[]{rec.getName(), rec.getCurrentRun(), rec.getTableCount(), rec.getRunDate()});
            }
            csvWriter.writeAll(records);
            csvWriter.flush();

            long contentLength = stream.toByteArray().length;

            //Initialize S3 client
            S3Client s3 = S3Client.builder().region(region).build();

            //Initialize PutObjectRequest by providing key name in datetime format
            PutObjectRequest objectRequest = PutObjectRequest.builder().bucket(bucketName)
                    .key(dynamoTableName+"/"+dynamoTableName+"-"+LocalDateTime.now()+".csv")
                    .contentLength(contentLength).build();

            //S3Client putobject API call to store object in S3 bucket
            s3.putObject(objectRequest, RequestBody.fromBytes(stream.toByteArray()));
        } catch (DynamoDbException e) {
            logger.log("DynamoDbException occurred :"+e.getMessage()+ " for entity :"+dynamoTableName);
            System.exit(1);
        } catch (IOException e) {
            logger.log("IOException occurred :"+e.getMessage()+" for entity :"+dynamoTableName);
            throw new RuntimeException(e);
        }
        logger.log("PersistEntity.writeAllItems - Done for Entity :"+dynamoTableName);
    }

    /**
     * Model the DynamoDB table to Entity class to track entity updates for run of CDAC
     */
    @DynamoDbBean
    public static class Entity {
        private String name;
        private String lastSuccessfulWritePq;
        private String currentRun;
        private String tableCount;
        private String runDate;

        @DynamoDbPartitionKey
        public String getName() {   return this.name;}
        public void setName(String name) {
            this.name = name;
        }

        public String getLastSuccessfulWritePq() {    return this.lastSuccessfulWritePq;}
        public void setLastSuccessfulWritePq(String lastSuccessfulWritePq) {
            this.lastSuccessfulWritePq = lastSuccessfulWritePq;
        }

        public String getCurrentRun() {
            return this.currentRun;
        }
        public void setCurrentRun(String currentRun) {
            this.currentRun = currentRun;
        }

        public String getTableCount() {
            return this.tableCount;
        }
        public void setTableCount(String tableCount) {
            this.tableCount = tableCount;
        }

        public String getRunDate() {
            return runDate;
        }
        public void setRunDate(String runDate) {
            this.runDate = runDate;
        }
    }

    /**
     * DynamoDB table model class to backing up RDS database
     */
    @DynamoDbBean
    public static class ClaimBackUp {

        @DynamoDbPartitionKey
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getLastSuccessfulWrite() {
            return lastSuccessfulWrite;
        }

        public void setLastSuccessfulWrite(String lastSuccessfulWrite) {
            this.lastSuccessfulWrite = lastSuccessfulWrite;
        }

        private String name;
        private String lastSuccessfulWrite;
    }

    /**
     *
     * @param dynamoTableName
     * @param lastSuccessfulWrite
     * @param logger
     * @param client
     */
    public void writeBackUpRecord(String dynamoTableName,String lastSuccessfulWrite, LambdaLogger logger, SsmClient client) {
        try{
            // Create a DynamoDbTable object
            DynamoDbTable<ClaimBackUp> entityDynamoDbTable = enhancedClient.table(dynamoTableName, TableSchema.fromBean(ClaimBackUp.class));
            Key key = Key.builder()
                    .partitionValue(dynamoTableName)
                    .build();
            ClaimBackUp existingRecord = entityDynamoDbTable.getItem(r -> r.key(key));
            logger.log("PersistentEntity.writeBackUpRecord - record for database "+dynamoTableName);
            if(existingRecord != null){
                //Update existing entity data
                existingRecord.setLastSuccessfulWrite(lastSuccessfulWrite);
                entityDynamoDbTable.updateItem(r -> r.item(existingRecord));
                logger.log("PersistentEntity.writeBackUpRecord - completed updating record for entity "+dynamoTableName);

            } else {
                // Populate the table
                ClaimBackUp entityRecord = new ClaimBackUp();
                entityRecord.setName(dynamoTableName);
                entityRecord.setLastSuccessfulWrite(lastSuccessfulWrite);
                // Put the table data into a DynamoDB table
                entityDynamoDbTable.putItem(entityRecord);
                logger.log("PersistentEntity.writeBackUpRecord - completed inserting record for entity "+dynamoTableName);
            }
        }  catch (DynamoDbException e) {
            logger.log("Exception occurred while upserting dynamoDB :"+e.getMessage());
            System.exit(1);
        }
        logger.log("PersistentEntity.writeBackUpRecord - done "+dynamoTableName);
    }

    /**
     *
     * @param logger
     * @param dynamoTableName
     * @return
     */
    public ClaimBackUp getLastRunTime(LambdaLogger logger, String dynamoTableName) {
        ClaimBackUp result = null;
        try {
            DynamoDbTable<ClaimBackUp> table = enhancedClient.table(dynamoTableName, TableSchema.fromBean(ClaimBackUp.class));
            Key key = Key.builder()
                    .partitionValue(dynamoTableName)
                    .build();

            // Get the item by using the key.
            result = (ClaimBackUp) table.getItem(
                    (GetItemEnhancedRequest.Builder requestBuilder) -> requestBuilder.key(key));

        } catch (DynamoDbException e) {
            logger.log("Exception occurred while getting item from dynamoDB :"+e.getMessage());
            System.exit(1);
        }
        //returning current run from entity recon table so that it can be updated as lastrun in the next execution
        if(result!= null && result.getLastSuccessfulWrite()!= null && !result.getLastSuccessfulWrite().isEmpty()){
            logger.log("******* The table Name is " + result.getName());
            return result;
        }
        return null;
    }
}
