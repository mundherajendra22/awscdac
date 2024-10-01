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
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;
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
public class PersistEntityChange {
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
     * @param stepFunctionExecutionId
     * @param entityName
     * @param logger
     * @param client
     */
    public void putRecord(String dynamoTableName, String entityName, String stepFunctionExecutionId, String stepFunctionStartExecution, LambdaLogger logger, SsmClient client) {
        try {
            // Create a DynamoDbTable object
            DynamoDbTable<EntityChange> entityDynamoDbTable = enhancedClient.table(dynamoTableName, TableSchema.fromBean(EntityChange.class));

            logger.log("PersistentEntity.putRecord - record for entity "+entityName);

            EntityChange entityChangeRecord = new EntityChange();
            entityChangeRecord.setStepFunctionExecutionId(stepFunctionExecutionId);
            entityChangeRecord.setStepFunctionStartExecution(stepFunctionStartExecution);
            entityChangeRecord.setName(entityName);

            // Put the entity data into a DynamoDB table
            entityDynamoDbTable.putItem(entityChangeRecord);
            logger.log("PersistentEntityChange.putRecord - completed inserting record for entity "+entityName);

        } catch (DynamoDbException e) {
            logger.log("Exception occurred while upserting dynamoDB :"+e.getMessage());
            System.exit(1);
        }
        logger.log("PersistentEntity.putRecord - done "+entityName);
    }


    /**
     * Model the DynamoDB table to Entity class to track entity updates for run of CDAC
     */

    @DynamoDbBean
    public static class EntityChange {

        private String stepFunctionExecutionId;
        private String stepFunctionStartExecution;
        private String name;

        @DynamoDbPartitionKey
        public String getStepFunctionExecutionId() {   return this.stepFunctionExecutionId;}
        public void setStepFunctionExecutionId(String stepFunctionExecutionId) {
            this.stepFunctionExecutionId = stepFunctionExecutionId;
        }

        @DynamoDbSortKey
        public String getName() {   return this.name;}
        public void setName(String name) {
            this.name = name;
        }

        public String getStepFunctionStartExecution() {   return this.stepFunctionStartExecution;}
        public void setStepFunctionStartExecution(String stepFunctionStartExecution) {
            this.stepFunctionStartExecution = stepFunctionStartExecution;
        }

    }

  }

