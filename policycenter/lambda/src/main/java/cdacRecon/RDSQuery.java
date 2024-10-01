package cdacRecon;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.lambda.powertools.parameters.ParamManager;
import software.amazon.lambda.powertools.parameters.SSMProvider;

import java.net.URI;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Helper class that interacts with RDS
 */
public class RDSQuery {

    //Initialize configuration class to get properties
    Configuration conf = new Configuration();
    String lambdaParam = conf.getLambdaParamPrefix();
    Region region = Region.US_EAST_1;


    /**
     * Supporting function that takes in tableName, logger, SsmClient (from AWS Lambda PowerTools) and the Guidewire product
     * The function returns count of rows in table
     *
     * @param tableName
     * @param logger
     * @param client
     * @param productName
     * @return count of rows in tableName as integer
     * @throws SQLException
     */
    public Integer getTableCount(Connection con, String tableName, LambdaLogger logger, SsmClient client, String productName) throws SQLException {
        SSMProvider ssmProvider = ParamManager.getSsmProvider(client);
        //Retrieve JDBC parameters
        String jdbcSch = lambdaParam + "/jdbcSchemaRaw";
        String jdbcSchemaRaw = ssmProvider.get(jdbcSch);

        PreparedStatement statement = null;
        Integer count = 0;
        try {
            //Get count of rows for the provided tableName. This query has been tested against Amazon RDS SQL Server
            // Please note the query may have to be changed if the database type changes
            statement = con.prepareStatement("select count(*) from " + con.getCatalog() + "." + jdbcSchemaRaw.toUpperCase() + "." + tableName.toUpperCase());
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            count = resultSet.getInt(1);
            logger.log("TableCount for table :" + tableName + " is " + count + " in Catalog :" + con.getCatalog() + " under Schema :" + con.getSchema());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
        }
        return count;
    }

    /**
     * Supporting function that takes in logger, SsmClient (from AWS Lambda PowerTools) and the Guidewire product for which
     * the dbBackUp has to be started. The function returns taskId of the DBBAckup that is returned by AWS StoredProcedure
     *
     * @param logger
     * @param client
     * @param productName
     * @return taskId
     * @throws SQLException
     */
    public String startDBBackUp(LambdaLogger logger, SsmClient client, String productName) throws SQLException {
        SSMProvider ssmProvider = ParamManager.getSsmProvider(client);

        //jdbcUrl parameter for the respective GW Application. This is the URL for RDS SQL Server
        String jdbcUrl = ssmProvider.get(lambdaParam + "/jdbcUrl");
        logger.log("JDBC URL : " + jdbcUrl);

        //getting jdbc credentials from AWS Secrets Manager
        String creds[] = getRDSPassword(ssmProvider);

        //jdbcUsername parameter for the respective GW Application. This is the username of the master user in RDS SQL Server
        String jdbcUsername = creds[0];

        //jdbcPassword parameter for the respective GW Application. This is the password of master user for RDS SQL Server
        String jdbcPassword = creds[1];

        //jdbcSchemaRaw parameter for the respective GW Application. This is the schema/database name for RDS SQL Server
        String jdbcSchemaRaw = ssmProvider.get(lambdaParam + "/jdbcSchemaRaw");

        //S3 bucket where the respective RDS Sql Server Backup of GW Application will be placed.
        String dbBackUpBucket = ssmProvider.get(lambdaParam + "/dbbucket");
        Connection con = null;
        CallableStatement statement = null;
        String taskId = "";

        try {
            con = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
            con.setAutoCommit(false);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd'T'HHmmss");
            sdf.setTimeZone(TimeZone.getDefault());
            String timestampInfo = sdf.format(Calendar.getInstance().getTime());

            // Call RDS StoredProcedure to initiate db backup by specifying s3 bucket arn and filename.
            // Backup filename format is yyyyMMddHHmmss.bak
            String backUpSP = "{call msdb.dbo.rds_backup_database(@source_db_name='" + jdbcSchemaRaw + "',@s3_arn_to_backup_to='arn:aws:s3:::" + dbBackUpBucket + "/" + productName + "/" + timestampInfo + ".bak',@type='FULL')}";
            logger.log("Backup PreparedStatement :" + backUpSP);
            statement = con.prepareCall(backUpSP);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                con.commit();

                //Get taskId of db backup process
                taskId = String.valueOf(resultSet.getInt("task_id"));
                logger.log("DB Back up started for " + productName + " with task_id :" + taskId);
            } else {
                logger.log("DB Back up DID NOT Start for " + productName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (con != null && !con.isClosed()) {
                con.close();
            }
        }
        return taskId;
    }

    /**
     * Supporting function that takes in logger, SsmClient (from AWS Lambda PowerTools), the Guidewire product for which
     * the dbBackUp was initiated and taskId of the back up process.
     * The function returns status of back up process for the taskId of the DBBackup
     *
     * @param logger
     * @param client
     * @param productName - used to retrieve parameters
     * @param taskId      - used to get status of a task
     * @return string - status of DBBackup
     * @throws SQLException
     */
    public String dBBackUpStatus(LambdaLogger logger, SsmClient client, String productName, String taskId) throws SQLException {
        SSMProvider ssmProvider = ParamManager.getSsmProvider(client);

        //Retrieve jdbc parameters from AWS Systems Manager
        String jdbcUrl = ssmProvider.get(lambdaParam + "/jdbcUrl");
        String creds[] = getRDSPassword(ssmProvider);
        String jdbcUsername = creds[0];
        String jdbcPassword = creds[1];
        Connection con = null;
        CallableStatement statement = null;
        String lifecycle = "";

        try {
            //JDBC connection
            con = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
            con.setAutoCommit(false);

            //Execute system stored procedure to get status/lifecycle of taskId
            String backUpStatSP = "{call msdb.dbo.rds_task_status(@task_id=";
            logger.log("BackupStatus PreparedStatement :" + backUpStatSP);
            statement = con.prepareCall(backUpStatSP + taskId + ")}");
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                lifecycle = resultSet.getString("lifecycle");
                logger.log("DB Back up for " + productName + " with taskId :" + taskId + " has status :" + lifecycle);
            } else {
                logger.log("DB Back up for " + productName + " with taskId :" + taskId + " has invalid status");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (con != null && !con.isClosed()) {
                con.close();
            }
        }
        return lifecycle;
    }

    /**
     * Supporting function that takes in logger, SSMProvider (from AWS Lambda PowerTools), the Guidewire product for which
     * a database has to be created. This database is the output for AWS CDAC as chosen by customer.
     * The database is typically created once before running AWS CDAC
     *
     * @param logger
     * @param client
     * @param product
     * @return boolean value
     * @throws SQLException
     */
    public boolean createDatabase(LambdaLogger logger, SsmClient client, String product) throws SQLException {
        SSMProvider ssmProvider = ParamManager.getSsmProvider(client);
        //Retrieve jdbc parameters for the guidewire product
        String jdbcUrl = ssmProvider.get(lambdaParam + "/jdbcUrl");
        String[] creds = getRDSPassword(ssmProvider);
        String jdbcUsername = creds[0];
        String jdbcPassword = creds[1];
        ;
        Connection con = null;
        PreparedStatement statement = null;
        String productName = ssmProvider.get(product);
        String schemaRaw = ssmProvider.get(lambdaParam + "/jdbcSchemaRaw");
        String schemaMerged = ssmProvider.get(lambdaParam + "/jdbcSchemaMerged");

        //S3 bucket where the respective RDS Sql Server Backup of GW Application will be placed.
        String dbBackUpBucket = ssmProvider.get(lambdaParam + "/dbbucket");
        Boolean result = false;
        try {
            //JDBC connection
            Class.forName("org.postgresql.Driver");
            con = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
            con.setAutoCommit(false);

            //drop raw database if the dropDBParam is set to true in AWS Systems Manager
            String dropDBParam = ssmProvider.get(lambdaParam + "/dropDBParam");
            logger.log("Drop db Parameter :" + dropDBParam);
            if (Boolean.valueOf(dropDBParam)) {
                statement = con.prepareStatement("drop schema " + schemaRaw + " CASCADE;");
                result = statement.execute();
                logger.log("Completed db drop :" + result);
            }
            //create raw database for the guidewire product passed in function parameter
            statement = con.prepareStatement("create schema " + schemaRaw + ";");
            result = statement.execute();
            logger.log("Completed db creation :" + result + " for database :" + schemaRaw);

            //drop merged database if the dropDBParam is set to true in AWS Systems Manager
            if (Boolean.valueOf(dropDBParam)) {
                statement = con.prepareStatement("drop schema " + schemaMerged + " CASCADE;");
                result = statement.execute();
                logger.log("Completed db drop :" + result);
            }
            //create merged database for the guidewire product passed in function parameter
            statement = con.prepareStatement("create schema " + schemaMerged + ";");
            result = statement.execute();
            logger.log("Completed db creation :" + result + " for database :" + schemaMerged);

            //set up database compression to reduce db backup size
            /*statement = con.prepareStatement("exec rdsadmin..rds_set_configuration 'S3 backup compression', 'true';");
            result = statement.execute();
            con.commit();
            logger.log("Completed db compression setup :"+result+ " for database :"+tableName);

            //this is applicable only for Amazon RDS SQL Server. This is a system stored procedure that creates
            // transaction log backup allowing for differential backup
            //https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER.SQLServer.AddlFeat.TransactionLogAccess.html
            statement = con.prepareStatement("exec msdb.dbo.rds_tlog_copy_setup @target_s3_arn='arn:aws:s3:::"+dbBackUpBucket+"/"+productName+"/transactionLogs/';");
            result = statement.execute();*/
            con.commit();
            logger.log("Completed transaction log setup :" + result);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (con != null && !con.isClosed()) {
                con.close();
            }
        }
        return result;
    }

    public String[] getRDSPassword(SSMProvider ssmProvider) {
        try {
            //SSM getParameter request for s3 VPCEndpoint URL
            String endpointURL = ssmProvider.get(conf.getLambdaParamPrefix()+"/secretsEndpointURL");
            SecretsManagerClient client = SecretsManagerClient.builder().region(region)
                    .endpointOverride(new URI(endpointURL))
                    .build();
            GetSecretValueRequest request = GetSecretValueRequest.builder().secretId(ssmProvider.get(conf.getLambdaParamPrefix() + "/RDSSecretName")).build();
            GetSecretValueResponse response;
            response = client.getSecretValue(request);
            JSONObject jsonObject = (JSONObject) JSONValue.parse(response.secretString());
            return new String[]{(String) jsonObject.get("username"), (String) jsonObject.get("password")};
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
