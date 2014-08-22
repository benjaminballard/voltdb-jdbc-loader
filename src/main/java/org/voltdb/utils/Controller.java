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
    private final String voltProcedure;
    private final Callback callback;
    private final Logger logger;
    protected Producer[] producers;  //producers created in array to establish simultaneous pages
    protected Consumer[] consumers;  //ditto
    protected Statement[] jdbcStmts; //statements arrayed- one for each page
    protected boolean signal = true;  //signal to continue
    protected boolean signalProducer = true;  //signal for producer to continue
    protected int producersFinished = 0;  //counter to coordinate page completion and full data completion
    protected int consumersFinished = 0;  //ditto
    protected String base;

    public Controller(Client client, Producer[] pr, Consumer[] cr, String sourceSelectQuery, String voltProcedure, Config config, int pages, String base) throws SQLException {
        this.logger = LoggerFactory.getLogger(Callback.class);
        this.voltProcedure = voltProcedure;
        this.callback = new Callback(this.voltProcedure, config.maxErrors, this);

        System.out.println("Connecting to source database with url: " + config.jdbcurl);
        logger.info("Connecting to source database with url: " + config.jdbcurl);
        Connection conn = DriverManager.getConnection(config.jdbcurl, config.jdbcuser, config.jdbcpassword);
        jdbcStmts = new Statement[pages];
        for (int i = 0; i < pages; i++) {
            //create individual, identical statements for each page (necessary for simultaneous counting)
            jdbcStmts[i] = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            jdbcStmts[i].setFetchSize(config.fetchsize);
        }

        System.out.println("Querying source database: " + sourceSelectQuery);
        logger.info("Querying source database: " + sourceSelectQuery);
        this.t1 = System.currentTimeMillis();
//        System.out.println(sourceSelectQuery);
        this.t2 = System.currentTimeMillis();
        float sec1 = (t2 - t1) / 1000.0f;
        //create several producers/consumers for simultaneous running
        this.producers = new Producer[pages];
        this.consumers = new Consumer[pages];

        System.out.println("Query took " + sec1 + " seconds to return first row, with fetch size of " + config.fetchsize + " records.");
        logger.info("Query took %.3f seconds to return first row, with fetch size of %s records.%n", sec1, config.fetchsize);

        this.monitor = new Monitor<ArrayList<Object[]>>(config.maxQueueSize);

        for (int i = 0; i < pages; i++) {
            System.out.println("About to create producers and consumers for " + i + ";");
            //set each producer/consumer equivalent to the corresponding sourceReader/destinationWriter with correct inputs
            producers[i] = pr[i].set(this, monitor, sourceSelectQuery, jdbcStmts[i], client, voltProcedure, callback, config, i, pages);
            consumers[i] = cr[i].set(this, monitor, client, voltProcedure, callback, jdbcStmts[i], config, i, pages);
            System.out.println("Producers and consumers for " + i + " created!");
        }
        this.base = base;
    }

    @Override
    public String call() throws Exception {
        try {
            //run producers and consumers on new individual threads
            for (int i = 0; i < producers.length; i++) {
                System.out.println("Calling producer-consumer pair #" + i + ";");
                producers[i].start();
                consumers[i].start();
                System.out.println("Producer-consumer pair " + i + " created!");
            }
            while (signalProducer() || signal()) {
                Thread.sleep(100);
                for (int i = 0; i < producers.length; i++) {
                    producers[i].join();
                    consumers[i].join();
                }
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
        Statement jdbcStmt;
        ResultSet rs;
        VoltTable[] resultArray;
        VoltTable results;
        Config config;
        int iteration; //id number of this producer within producers[]
        int pages; //number of producer-consumer pairs running
        int columns; //data table columns
        boolean endProducerFlag = false; //flag to indicate end of a particular producer

        public Producer set(Controller<T> controller, Monitor<T> monitor, String query, Statement jdbcStmt, Client client, String voltProcedure, Callback callback, Config config, int iteration, int pages) {
            this.controller = controller;
            this.monitor = monitor;
            this.jdbcStmt = jdbcStmt;
            this.query = query;
            this.client = client;
            tableName = voltProcedure;
            this.callback = callback;
            this.config = config;
            this.iteration = iteration;
            this.pages = pages;

            this.setName("Producer");

            return this;
        }

        public void run() {
            if (config.srisvoltdb) {
                System.out.println("Sr begun");
                try {
                    //referencing properties via query
                    if (query.contains("<") && query.contains(">")) {
                        int bracketOpen = query.indexOf("<");
                        int bracketClose = query.indexOf(">");
                        String orderCol = query.substring(bracketOpen + 1, bracketClose);
                        query = query.replace("<" + orderCol + ">", "");
                        if (bracketOpen != -1 && bracketClose != -1) {
                            query = query.replace(";", "ORDER BY " + orderCol + ";");
                        }
                    }
                    System.out.println("Creating resultArray for " + iteration + "! Query: " + query);
                    resultArray = client.callProcedure("@AdHoc", query).getResults();
                    System.out.println("ResultArray for " + iteration + " created!");

                } catch (Exception e) {
                    System.out.println("Result Array formation failure!");
                    e.printStackTrace();
                }
                if (resultArray == null) {
                    results = null;
                } else {
                    results = resultArray[0];
                    //advance to current point, endpoint of previous iteration (running simultaneously)
                    results.advanceRow();
                    results.advanceToRow((iteration * config.pageSize) - 1);
                    System.out.println("Advancetorow complete!");
                }
            } else if (!config.srisvoltdb) {
                try {
                    //referencing properties via query
                    if (query.contains("<") && query.contains(">")) {
                        int bracketOpen = query.indexOf("<");
                        int bracketClose = query.indexOf(">");
                        String orderCol = query.substring(bracketOpen + 1, bracketClose);
                        query = query.replace("<" + orderCol + ">", "");
                        if (bracketOpen != -1 && bracketClose != -1) {
                            query = query.replace(";", " ORDER BY " + orderCol + ";");
                        }
                    }
                    if (controller.base.contains("postgres") && config.isPaginated) {
                        //set limit and offset to query. WILL ONLY WORK IN POSTGRES. Need alternative method for Netezza.
                        query = query.replace(";", " limit " + config.pageSize + " offset " + config.pageSize * iteration + ";");
//                    System.out.println("Iteration: " + iteration + ", query: " + limitedQuery.toUpperCase());
                    }
                    rs = jdbcStmt.executeQuery(query.toUpperCase());
                    columns = rs.getMetaData().getColumnCount();
                } catch (SQLException sql) {
                    System.out.println("SQLException creating resultSet!");
                    sql.printStackTrace();
                }
            }
            System.out.println("Signal: " + controller.signal + ". ProducerSignal: " + controller.signalProducer + ".");
            while (controller.signal() && controller.signalProducer()) {  // end thread when there is nothing more that producer can do
//                System.out.println("Beginning producerTask!");
                producerTask();
                //end page (but not all producers) once pageSize is reached
                if (endProducerFlag) {
                    System.out.println("Producer flag for " + iteration + " has been flown!");
                    //increment number of completed producers
                    controller.producersFinished++;
                    System.out.println("Producers completed: " + controller.producersFinished);
                    //end signalProducer when all producers are complete
                    if (controller.producersFinished == pages) {
                        System.out.println("Kill producer signal!");
                        controller.signalProducer(false);
                    }
                    break;
                }
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
        Boolean endConsumerFlag = false;
        int iteration; //id number of this consumer within consumers[]
        int pages; //number of producer-consumer pairs running
        int tasks = 0; //upcounter of rows being put through sr

        public Consumer<T> set(Controller controller, Monitor<T> monitor, Client client, String voltProcedure, Callback callback, Statement jdbcStmt, Config config, int iteration, int pages) {
            this.controller = controller;
            this.monitor = monitor;
            this.client = client;
            this.procName = voltProcedure;
            this.callback = callback;
            this.setName("Consumer");
            this.jdbcStmt = jdbcStmt;
            this.config = config;
            this.iteration = iteration;
            this.pages = pages;
            sb = new StringBuilder();

            //set up beginning of SQL INSERT, based on tables' names
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
            System.out.println("Cr begun");
            System.out.println("waitMethod complete for " + iteration + "!");

            while (controller.signal()) {
                consumerTask();
                //end page (but not all consumers) once pageSize is reached
                if (endConsumerFlag) {
                    System.out.println("Consumer flag for " + iteration + " has been flown!");
                    //increment number of completed consumers
                    controller.consumersFinished++;
                    System.out.println("Consumers finished: " + controller.consumersFinished);
                    //end signal when all consumers are complete
                    if (controller.consumersFinished == pages) {
                        System.out.println("Kill signal!");
                        controller.signal(false);
                    }
                    break;
                }
            }
        }

        protected abstract void consumerTask();
    }
}
