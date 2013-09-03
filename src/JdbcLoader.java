package jdbcloader;

import org.voltdb.*;
import org.voltdb.client.*;
import java.sql.*;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;

//import java.text.SimpleDateFormat;


public class JdbcLoader {


    Class c;
    Connection conn;
    Calendar cal;
    Client client;
    LoaderConfig config;
    
    public JdbcLoader(LoaderConfig config) {
        this.config = config;
        cal = Calendar.getInstance(); 
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public void connectToSource() throws Exception {
        // load JDBC driver
        c = Class.forName(config.jdbcdriver);
        System.out.println("Connecting to source database with url: " + config.jdbcurl);
        conn = DriverManager.getConnection(config.jdbcurl,config.jdbcuser,config.jdbcpassword); 
    }

    public void connectToVoltDB() throws InterruptedException {
        System.out.println("Connecting to VoltDB on: " + config.volt_servers);

        ClientConfig cc = new ClientConfig(config.volt_user, config.volt_password);
        client = ClientFactory.createClient(cc);

        String servers = config.volt_servers;
        String[] serverArray = servers.split(",");
        final CountDownLatch connections = new CountDownLatch(serverArray.length);

        // use a new thread to connect to each server
        for (final String server : serverArray) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    connectVoltNode(server);
                    connections.countDown();
                }
            }).start();
        }
        // block until all have connected
        connections.await();
    }

    void connectVoltNode(String server) {
        int sleep = 1000;
        while (true) {
            try {
                client.createConnection(server);
                break;
            }
            catch (Exception e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try { Thread.sleep(sleep); } catch (Exception interruted) {}
                if (sleep < 8000) sleep += sleep;
            }
        }
        System.out.printf("Connected to VoltDB node at: %s.\n", server);
    }

    public void close() throws Exception {
        conn.close();
        client.drain();
        client.close();
        System.out.println("closed connections");
    }

    public void loadTable(String tableName, String procName) throws SQLException, Exception {

        long t1 = System.currentTimeMillis();

        // if procName not provided, use the default VoltDB TABLENAME.insert procedure
        if (procName.length() == 0) {
            procName = tableName.toUpperCase() + ".insert";
        }

        // query the table
        String jdbcSelect = "SELECT * FROM " + tableName + ";";
        System.out.println("Querying source database: " + jdbcSelect);
        Statement jdbcStmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        jdbcStmt.setFetchSize(config.fetchsize);
        ResultSet rs = jdbcStmt.executeQuery(jdbcSelect);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columns = rsmd.getColumnCount();

        long t2 = System.currentTimeMillis();
        float sec1 = (t2-t1)/1000.0f;
        System.out.format("Query took %.3f seconds to return first row, with fetch size of %s records.%n",sec1,config.fetchsize);

        int records=0;
        while (rs.next()){
            records++;

            // get one record of data as an Object[]
            Object[] columnValues = new Object[columns];
            for (int i=0; i<columns; i++) {
                columnValues[i] = rs.getObject(i+1);
            }

            // insert the record 
            client.callProcedure(new LoaderCallback(procName),
                                 procName,
                                 columnValues
                                 );

            if (records % 10000 == 0) {
                System.out.println("Sent " + records + " requests");
            }
        }

        long t3 = System.currentTimeMillis();
        float sec2 = (t3-t2)/1000.0f;
        float tps = records/sec2;
        System.out.format("Sent %d requests in %.3f seconds at a rate of %f TPS.%n",records, sec2,tps);
        client.drain();
        LoaderCallback.printProcedureResults(procName);        
    }


    public static void main(String[] args) throws Exception {

        // process args
        LoaderConfig cArgs = LoaderConfig.getConfig("JdbcLoader",args);
        
        JdbcLoader loader = new JdbcLoader(cArgs);
        loader.connectToSource();
        loader.connectToVoltDB();

        loader.loadTable(cArgs.tablename, cArgs.procname);

        loader.close();

    }
 
}
