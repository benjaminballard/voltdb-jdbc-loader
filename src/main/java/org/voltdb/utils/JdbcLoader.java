package org.voltdb.utils;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

//import java.text.SimpleDateFormat;


public class JdbcLoader {


    Class c;
    Connection conn;
    Calendar cal;
    Client client;
    LoaderConfig config;
    Statement jdbcStmt;
    int records = 0;


    public JdbcLoader(LoaderConfig config) {
        this.config = config;
        cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public void connectToSource() throws Exception {
        // load JDBC driver
        c = Class.forName(config.jdbcdriver);
        System.out.println("Connecting to source database with url: " + config.jdbcurl);
        conn = DriverManager.getConnection(config.jdbcurl, config.jdbcuser, config.jdbcpassword);
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
            } catch (Exception e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try {
                    Thread.sleep(sleep);
                } catch (Exception interruted) {
                }
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

    public void loadTables(String tableNames, String procNames) throws SQLException, IOException, InterruptedException {
        String[] tableNameArray = tableNames != null && !"".equals(tableNames) ? tableNames.split(",") : null;
        String[] procNameArray = procNames != null && !"".equals(procNames) ? procNames.split(",") : null;

        for (int j = 0; j < tableNameArray.length && tableNameArray != null; j++) {
            String tableName = tableNameArray[j];
            String procName = procNameArray != null ? procNameArray[j] : "";

            // if procName not provided, use the default VoltDB TABLENAME.insert procedure
            if (procName.length() == 0) {
                if (tableName.contains("..")) {
                    procName = tableName.split("\\.\\.")[1].toUpperCase() + ".insert";
                } else {
                    procName = tableName.toUpperCase() + ".insert";
                }
            }

            // query the table
            String jdbcSelect = "SELECT * FROM " + tableName + ";";

            copyData(jdbcSelect, procName);
        }
    }

    public void load(String queryFile) throws SQLException, IOException, InterruptedException {
        Properties properties = new Properties();

        properties.load(new FileInputStream(queryFile));

        Set<String> keys = properties.stringPropertyNames();

        for (String key : keys) {
            if (key.startsWith("param_")) {
                continue;
            }

            String query = properties.getProperty(key);

            while (query.contains("[:")) {
                String param = query.substring(query.indexOf("[:") + 2, query.indexOf("]"));
                System.out.println("Param:" + param);

                query = query.replaceFirst("\\[\\:" + param + "\\]", properties.getProperty(param));
            }

            copyData(query, key.toUpperCase() + ".insert");
        }
    }

    private void copyData(String sourceSelectQuery, String voltProcedure) throws SQLException, IOException, InterruptedException {
        System.out.println("Querying source database: " + sourceSelectQuery);
        long t1 = System.currentTimeMillis();
        ResultSet rs = jdbcStmt.executeQuery(sourceSelectQuery);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columns = rsmd.getColumnCount();

        long t2 = System.currentTimeMillis();
        float sec1 = (t2 - t1) / 1000.0f;
        System.out.format("Query took %.3f seconds to return first row, with fetch size of %s records.%n", sec1, config.fetchsize);

        ParallelProcessor.Monitor mr = new ParallelProcessor.Monitor<ArrayList<Object[]>>(config.maxQueueSize);
        SourceReader sr = new SourceReader(mr, rs, columns);
        DestinationWriter dr = new DestinationWriter(mr, client, voltProcedure);

        ParallelProcessor processor = new ParallelProcessor<ArrayList<Object[]>>();

        processor.process(sr, dr);

        long t3 = System.currentTimeMillis();
        float sec2 = (t3 - t2) / 1000.0f;
        float tps = records / sec2;

        System.out.format("Sent %d requests in %.3f seconds at a rate of %f TPS.%n", records, sec2, tps);
        LoaderCallback.printProcedureResults(voltProcedure);

        records = 0;
    }


    private class SourceReader extends ParallelProcessor.Producer<ArrayList<Object[]>> {
        ResultSet rs;
        int columns;

        SourceReader(ParallelProcessor.Monitor<ArrayList<Object[]>> monitor, ResultSet rs, int columns) {
            super(monitor);
            this.rs = rs;
            this.columns = columns;
        }

        protected void producerTask() {
            ArrayList<Object[]> arrayList = new ArrayList<Object[]>();
            try {
                while (rs.next()) {
                    records++;

                    // get one record of data as an Object[]
                    Object[] columnValues = new Object[columns];
                    for (int i = 0; i < columns; i++) {
                        columnValues[i] = rs.getObject(i + 1);
                    }

                    arrayList.add(columnValues);

                    if (records % config.fetchsize == 0) {
                        System.out.println("Pulled " + config.fetchsize + " records from the source");

                        monitor.jobQueue.put(arrayList);

                        arrayList = new ArrayList<Object[]>();
                    }
                }

                System.out.println("Pulled " + records % config.fetchsize + " records from the source");

                monitor.jobQueue.put(arrayList);

                controller.signalProducer(false);
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                System.out.println(Thread.currentThread().getName() + " - unexpected exception");
                e.printStackTrace();
                controller.signal(false);
            }
        }
    }

    private class DestinationWriter extends ParallelProcessor.Consumer<ArrayList<Object[]>> {
        Client client;
        String voltProcedure;

        DestinationWriter(ParallelProcessor.Monitor<ArrayList<Object[]>> monitor, Client client, String voltProcedure) {
            super(monitor);
            this.client = client;
            this.voltProcedure = voltProcedure;
        }

        public void consumerTask() {
            // insert the record
            try {
                ArrayList<Object[]> arrayList = monitor.jobQueue.poll(config.maxWaitTime, TimeUnit.SECONDS);

                if(arrayList == null && !controller.signalProducer){
                    controller.signal(false);
                } else if (arrayList == null){
                    System.out.println(Thread.currentThread().getName() + " waited for " + config.maxWaitTime + " to retrieve new data. Hence ending the transfer now assuming that producer is stuck.");
                    controller.signal(false);
                } else {
                    for (Object[] columnValues : arrayList) {
                        client.callProcedure(new LoaderCallback(voltProcedure),
                                voltProcedure,
                                columnValues
                        );
                    }

                    System.out.println("Sent " + arrayList.size() + " requests");
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.out.println("Interrupted while " + Thread.currentThread().getName() + " waited for " + config.maxWaitTime + " to retrieve new data. Hence ending the transfer now");
                controller.signal(false);
            } catch (Exception e) {
                System.out.println(Thread.currentThread().getName() + " - unexpected exception");
                e.printStackTrace();
                controller.signal(false);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        // process args
        LoaderConfig cArgs = LoaderConfig.getConfig("JdbcLoader", args);

        JdbcLoader loader = new JdbcLoader(cArgs);
        loader.connectToSource();
        loader.connectToVoltDB();

        loader.jdbcStmt = loader.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        loader.jdbcStmt.setFetchSize(loader.config.fetchsize);

        if (cArgs.queriesFile.isEmpty()) {
            loader.loadTables(cArgs.tablename, cArgs.procname);
        } else {
            loader.load(cArgs.queriesFile);
        }

        loader.close();
    }

}
