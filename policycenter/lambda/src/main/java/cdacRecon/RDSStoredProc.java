package cdacRecon;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.lambda.powertools.parameters.SSMProvider;

import java.sql.*;

public class RDSStoredProc {
    Configuration conf = new Configuration();
    String lambdaParam = conf.getLambdaParamPrefix();

    public void executeMergedView(SSMProvider ssmProvider, LambdaLogger logger) {
        String jdbcURL = lambdaParam + "/jdbcUrl";
        String jdbcUrl = ssmProvider.get(jdbcURL);
        String creds[] = new RDSQuery().getRDSPassword(ssmProvider);
        String jdbcUsername = creds[0];
        String jdbcPassword = creds[1];
        CallableStatement callableStatement = null;
        PreparedStatement preparedStatement = null;
        Connection con = null;

        try {
            // JDBC Connection
            Class.forName("org.postgresql.Driver");
            con = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
            con.setAutoCommit(false);

            S3Client s3client = S3Client.builder().region(Region.US_EAST_1).build();

            // Build the getObjectRequest using bucket name and object key
            // Read Stored Procedure Sql file in S3
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(conf.getStoredProcBucket())
                    .key(conf.getStoredProcKey())
                    .build();

            String storedProc = s3client.getObjectAsBytes(getObjectRequest).asUtf8String();
            String storedProcSQL = storedProc.replace(conf.getStoredProcMergedView(),conf.getStoredProcSchema() + "." + conf.getStoredProcMergedView());

            //Create stored procedure on RDS
            preparedStatement = con.prepareStatement(storedProcSQL);
            logger.log("my stored proc is" + storedProcSQL);
            preparedStatement.execute();
            con.commit();

            // Call stored procedure
            String execStoredProc = "CALL" + " " + conf.getStoredProcSchema() + "." + conf.getStoredProcMergedView() + "(?, ?)";
            callableStatement = con.prepareCall(execStoredProc);
            callableStatement.setString(1, conf.getJdbcSchemaRaw());
            callableStatement.setString(2, conf.getJdbcSchemaMerged());
            logger.log("Stored Proc in Callable Statement :"+callableStatement);
            callableStatement.execute();
            con.commit();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (preparedStatement != null && !preparedStatement.isClosed()) {
                    preparedStatement.close();
                }
                if (callableStatement != null && !callableStatement.isClosed()) {
                    callableStatement.close();
                }
                if (con != null && !con.isClosed()) {
                    con.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}