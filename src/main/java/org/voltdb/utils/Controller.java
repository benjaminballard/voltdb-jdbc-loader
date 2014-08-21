package org.voltdb.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;

import java.sql.*;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

public class Controller<T> implements Callable {
    private final long t1, t2;
    private final Monitor<ArrayList<Object[]>> monitor;
    private final Statement jdbcStmt;
    private final String voltProcedure;
    private final Callback callback;
    private final Logger logger;
    private Producer producer;
    private Consumer consumer;
    protected boolean signal = true;  //signal to continue
    protected boolean signalProducer = true;  //signal for producer to continue

    public Controller(Client client, Producer pr, Consumer cr, String sourceSelectQuery, String voltProcedure, Config config) throws SQLException {
        this.logger = LoggerFactory.getLogger(Callback.class);
        this.voltProcedure = voltProcedure;
        this.callback = new Callback(this.voltProcedure, config.maxErrors, this);

        logger.info("Connecting to source database with url: " + config.jdbcurl);
        Connection conn = DriverManager.getConnection(config.jdbcurl, config.jdbcuser, config.jdbcpassword);
        this.jdbcStmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        this.jdbcStmt.setFetchSize(config.fetchsize);

        logger.info("Querying source database: " + sourceSelectQuery);
        this.t1 = System.currentTimeMillis();
//        System.out.println(sourceSelectQuery);
        ResultSet rs = jdbcStmt.executeQuery(sourceSelectQuery);
        int columns = rs.getMetaData().getColumnCount();
        this.t2 = System.currentTimeMillis();
        float sec1 = (t2 - t1) / 1000.0f;

        logger.info("Query took %.3f seconds to return first row, with fetch size of %s records.%n", sec1, config.fetchsize);

        this.monitor = new Monitor<ArrayList<Object[]>>(config.maxQueueSize);

        this.producer = pr.set(this, monitor, sourceSelectQuery, jdbcStmt, client, voltProcedure, callback, rs, columns, config);
        this.consumer = cr.set(this, monitor, client, voltProcedure, callback, jdbcStmt, config);
    }

    @Override
    public String call() throws Exception {
        try {
            producer.start();
            consumer.start();

            while (signalProducer() || signal()) {
                Thread.sleep(100);
                producer.join();
                consumer.join();
            }

            long t3 = System.currentTimeMillis();
            float sec2 = (t3 - t2) / 1000.0f;
            float tps = monitor.records / sec2;

            logger.info("Sent %d requests in %.3f seconds at a rate of %f TPS.%n", monitor.records, sec2, tps);
            callback.printProcedureResults(voltProcedure);
        } catch (InterruptedException ie) {
            logger.error("Producer has been interrupted"+ ie.getMessage());
            signal(false);
        }

        return voltProcedure;
    }

    protected void signal(boolean b) {
        this.signal = b;
    }

    protected boolean signal() {
        return signal;
    }

    public void signalProducer(boolean b) {
        this.signalProducer = b;
    }

    protected boolean signalProducer() {
        return signalProducer;
    }


    public static class Monitor<T> {
        BlockingQueue<T> jobQueue;
        public int records;

        Monitor(int maxQueueSize) {
            jobQueue = new LinkedBlockingQueue<T>(maxQueueSize);
        }
    }

    public static abstract class Producer<T> extends Thread {
        protected Monitor<T> monitor;
        protected Controller<T> controller;
        protected String query;
        Client client;
        String tableName;
        Callback callback;
        ResultSet rs;
        int columns;
        Statement jdbcStmt;
        VoltTable[] resultArray;
        VoltTable results;
        Config config;


        public Producer set(Controller<T> controller, Monitor<T> monitor, String query, Statement jdbcStmt, Client client, String voltProcedure, Callback callback, ResultSet rs, int columns, Config config) {
            this.controller = controller;
            this.monitor = monitor;
            this.rs = rs;
            this.columns = columns;
            this.jdbcStmt = jdbcStmt;
            this.query = query;
            this.client = client;
            this.tableName = voltProcedure;
            this.callback = callback;
            this.config = config;

            this.setName("Producer");

            return this;
        }

        public void run() {

            if (config.srisvoltdb) {
                try {
                    resultArray = client.callProcedure("@AdHoc", query).getResults();
                } catch (Exception e) {
                    System.out.println("Result Array formation failure!");
                }
                if (resultArray == null) {
                    results = null;
                } else {
                    results = resultArray[0];
                }
            }
            while (controller.signal() && controller.signalProducer()) {  // end thread when there is nothing more that producer can do
                producerTask();
            }
        }

        protected abstract void producerTask();
    }

    public static abstract class Consumer<T> extends Thread {
        protected Monitor<T> monitor;
        public Controller<T> controller;
        Client client;
        String procName;
        Callback callback;
        Statement jdbcStmt;
        Config config;
        StringBuilder sb;

        public Consumer<T> set(Controller controller, Monitor<T> monitor, Client client, String voltProcedure, Callback callback, Statement jdbcStmt, Config config) {
            this.controller = controller;
            this.monitor = monitor;
            this.client = client;
            this.procName = voltProcedure;
            this.callback = callback;
            this.setName("Consumer");
            this.jdbcStmt = jdbcStmt;
            this.config = config;
            sb = new StringBuilder();

            if (config.queriesFile.isEmpty()) {
                sb.append("INSERT INTO ").append(config.tables);
                sb.append(" (");
            } else {
                sb.append("INSERT INTO ").append(voltProcedure.replace(".insert", ""));
                sb.append(" (");
            }

            return this;
        }

        public void run() {
            while (controller.signal()) {
                consumerTask();
            }
        }

        protected abstract void consumerTask();
    }
}
