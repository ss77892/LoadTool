import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.CsvBulkLoadTool;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;

/**
 * Created by ssoldatov on 12/18/16.
 */
public class TestTable {

    private static final Log LOG = LogFactory.getLog(TestTable.class);

    public static void main (String[] args) throws Exception {
        String host = "localhost";
        if(args.length == 1) {
            host = args[0];
        }
        Connection conn = DriverManager.getConnection("jdbc:phoenix:" + host);

        final PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
        final HBaseAdmin admin = phxConn.getQueryServices().getAdmin();
        while(true) {
            String t2 = "DROP TABLE IF EXISTS G1";
            conn.createStatement().execute(t2);
            t2 = "CREATE TABLE IF NOT EXISTS G1 (ID INTEGER PRIMARY KEY, unsig_id UNSIGNED_INT, big_id BIGINT, unsig_long_id UNSIGNED_LONG, tiny_id TINYINT, unsig_tiny_id UNSIGNED_TINYINT, small_id SMALLINT, unsig_small_id UNSIGNED_SMALLINT, float_id FLOAT, unsig_float_id UNSIGNED_FLOAT, double_id DOUBLE, unsig_double_id UNSIGNED_DOUBLE, decimal_id DECIMAL, boolean_id BOOLEAN, time_id TIME, date_id DATE, timestamp_id TIMESTAMP, unsig_time_id TIME, unsig_date_id DATE, unsig_timestamp_id TIMESTAMP, varchar_id VARCHAR (30), char_id CHAR (30), binary_id VARCHAR (100), varbinary_id VARCHAR (100), array_id VARCHAR[])";
            conn.createStatement().execute(t2);
            CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
            csvBulkLoadTool.setConf(new Configuration());
//            Process proc = Runtime.getRuntime().exec("hadoop jar /usr/hdp/current/phoenix-client/phoenix-client.jar org.apache.phoenix.mapreduce.CsvBulkLoadTool --input /tmp/1.csv --table GIGANTIC_TABLE -d , -a ';'");
            ProcessBuilder pb = new ProcessBuilder("/usr/hdp/current/hadoop-client/bin/hadoop", "jar", "/usr/hdp/current/phoenix-client/phoenix-client.jar", "org.apache.phoenix.mapreduce.CsvBulkLoadTool",  "--input", "/tmp/1.csv", "--table", "G1", "-d", ",", "-a", "';'");
            pb.environment().put("HADOOP_CLASSPATH", "/etc/hbase/conf");
            pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
            Process p = pb.start();
            p.waitFor();
//            int exitCode = csvBulkLoadTool.run(new String[]{
//                    "--input", "/tmp/1.csv",
//                    "--table", "GIGANTIC_TABLE",
//                    "-d", ",",
//                    "-a", ";",
//                    "-q", "\"\"\""
//            });
//            assertEquals(0, exitCode);
//      t2 = "CREATE LOCAL INDEX L1 ON GIGANTIC_TABLE (big_id)";
//      phxConn.createStatement().execute(t2);
            for(int j = 0; j < 5; j++) {
                t2 = "DROP INDEX IF EXISTS L1_id on G1";
                phxConn.createStatement().execute(t2);
                t2 = "CREATE LOCAL INDEX L1_id ON G1 (char_id)";
                phxConn.createStatement().execute(t2);
//            t2 = "UPSERT INTO GIGANTIC_TABLE VALUES (900000,9000,900,90,32,32,416,15291,0.5,0.25,0.125,10" +
//                    ".7583463727,10.0100117798,False,'1971-07-07 20:25:12','1974-05-23 04:10:18','1983-09-27 11:53:11','2026-11-12 01:50:01.000567','1979-02-25 05:39:13.000567','1998-08-29 01:43:05.000567','Sample text extra','b','asdfasfasd','asdfasdf')";
//            conn.createStatement().execute(t2);
//            conn.commit();
//            t2 = "DELETE FROM  GIGANTIC_TABLE WHERE ID = 900000";
//            phxConn.createStatement().execute(t2);
//            conn.commit();

//            byte[] tableNameBytes = Bytes.toBytes("GIGANTIC_TABLE");
////      admin.split(tableNameBytes);
                String query = "SELECT count(*) FROM G1 WHERE char_id like '%a%'";
                boolean stopit = false;
                for (int i = 0; i < 5 ; i++) {
                    LOG.error("START QUERY");
                    ResultSet rs = phxConn.createStatement().executeQuery(query);
                    while (rs.next()) {
                        int result = rs.getInt(1);
                        if (result == 500000) {
                            LOG.error("RESULT : OK");
                        } else {
                            LOG.error("RESULT FAILURE!!!! : " + result);
                        }
                    }
                }
            }
        }

    }
}
