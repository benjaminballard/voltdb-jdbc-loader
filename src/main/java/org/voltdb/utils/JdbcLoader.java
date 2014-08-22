package org.voltdb.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;

public class JdbcLoader {
    Logger logger;
    Class c;
    Client client;
    Calendar cal;
    Config config;

    public JdbcLoader(Config config) {
        this.config = config;
        cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("EST"));
        logger = LoggerFactory.getLogger(JdbcLoader.class);
    }
    public void connectToSource() throws Exception {
        // load JDBC driver
        c = Class.forName(config.jdbcdriver);
    }

    public void loadTables(String tableNames, String procNames) throws SQLException, IOException, InterruptedException, ExecutionException {
        String[] tableNameArray = tableNames != null && !"".equals(tableNames) ? tableNames.split(",") : null;
        String[] procNameArray = procNames != null && !"".equals(procNames) ? procNames.split(",") : null;

        ExecutorService executor = Executors.newFixedThreadPool(tableNameArray.length * 3);
        CompletionService completion = new ExecutorCompletionService(executor);

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

            //create query to find count
            String countquery= jdbcSelect.replace("*", "COUNT(*)");
            int pages = 1;
            String base = "";
            if (config.srisvoltdb) {
                if (config.isPaginated) {
                    try {
                        //find count
                        if (countquery.contains("<") || countquery.contains(">")) {
                            int bracketOpen = countquery.indexOf("<");
                            int bracketClose = countquery.indexOf(">");
                            String orderCol = countquery.substring(bracketOpen + 1, bracketClose);
                            countquery = countquery.replace("<" + orderCol + ">", "");
                        }
                        VoltTable vcount = client.callProcedure("@AdHoc", countquery).getResults()[0];
                        int count = Integer.parseInt(vcount.toString());
                        //determine number of pages from total data and page size
                        pages = (int) Math.ceil((double) count / config.pageSize);
                        System.out.println(pages);
                    } catch (Exception e) {
                        System.out.println("Count formation failure!");
                    }
                }
            } else {
                    //find count
                Connection conn = DriverManager.getConnection(config.jdbcurl, config.jdbcuser, config.jdbcpassword);
                base = conn.getMetaData().getDatabaseProductName().toLowerCase();
                Statement jdbcStmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                if (countquery.contains("<") || countquery.contains(">")) {
                    int bracketOpen = countquery.indexOf("<");
                    int bracketClose = countquery.indexOf(">");
                    String orderCol = countquery.substring(bracketOpen + 1, bracketClose);
                    countquery = countquery.replace("<" + orderCol + ">", "");
                }
                ResultSet rcount = jdbcStmt.executeQuery(countquery);                  //determine number of pages from total data and page size
                if (base.contains("postgres") && config.isPaginated) {
                    int count = Integer.parseInt(rcount.toString());
                    pages = (int) Math.ceil((double) count / config.pageSize);
                }
            }

            //establish new SourceReaders and DestinationWriters for pages
            SourceReader[] sr = new SourceReader[pages];
            DestinationWriter[] cr = new DestinationWriter[pages];
            for (int i = 0; i < pages; i++) {
                sr[i] = new SourceReader();
                cr[i] = new DestinationWriter();
            }
            Controller processor = new Controller<ArrayList<Object[]>>(client, sr, cr, jdbcSelect, procName, config, pages, base);
            completion.submit(processor);
        }

        // wait for all tasks to complete.
        for (int i = 0; i < tableNameArray.length; ++i) {
            logger.info("****************" + completion.take().get() + " completed *****************"); // will block until the next sub task has completed.
        }
        executor.shutdown();
    }

    public void load(String queryFile, String modules, String tables) throws SQLException, IOException, InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(queryFile));

        Collection<String> keys = properties.stringPropertyNames();

        //Filtering by validating if property starts with any of the module names
        if(!Config.ALL.equalsIgnoreCase(modules)){
            keys = Util.filter(keys, "^(" + modules.replaceAll(Config.COMMA_SEPARATOR, Config.MODULE_SUFFIX) + ")" );
        }

        //Filtering by table names
        if(!Config.ALL.equalsIgnoreCase(tables)){
            keys = Util.filter(keys, "(" + tables.replaceAll(Config.COMMA_SEPARATOR, Config.TABLE_SUFFIX) + ")$" );
        }

        logger.info("The final modules and tables that are being considered" + keys.toString());

        ExecutorService executor = Executors.newFixedThreadPool(keys.size() * 3);
        CompletionService completion = new ExecutorCompletionService(executor);

        for (String key : keys) {
            String query = properties.getProperty(key);
            key = (key.contains(Config.DOT_SEPARATOR) ? key.substring(key.indexOf(Config.DOT_SEPARATOR)+ 1) : key);


            while (query.contains("[:")) {
                String param = query.substring(query.indexOf("[:") + 2, query.indexOf("]"));

                query = query.replaceFirst("\\[\\:" + param + "\\]", properties.getProperty(param));
            }
            int pages = 1;
            String base = "";
            if (config.srisvoltdb) {
                if (config.isPaginated) {
                    try {
                        //find count
                        String countquery = query;
                        if (countquery.contains("<") || countquery.contains(">")) {
                            int bracketOpen = countquery.indexOf("<");
                            int bracketClose = countquery.indexOf(">");
                            String orderCol = countquery.substring(bracketOpen + 1, bracketClose);
                            countquery = countquery.replace("<" + orderCol + ">", "");
                        }
                        VoltTable vcount = client.callProcedure("@AdHoc", countquery).getResults()[0];
                        int count = vcount.getRowCount();
                        pages = (int) Math.ceil((double) count / config.pageSize);
                    } catch (Exception e) {
                        System.out.println("Count formation failure!");
                    }
                }
                // set up data in order
            } else {
                //find count
                String countquery = query.replace("*", "COUNT(*)");
                Connection conn = DriverManager.getConnection(config.jdbcurl, config.jdbcuser, config.jdbcpassword);
                base = conn.getMetaData().getDatabaseProductName().toLowerCase();
                System.out.println("BASE: " + base);
                Statement jdbcStmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                if (countquery.contains("<") || countquery.contains(">")) {
                    int bracketOpen = countquery.indexOf("<");
                    int bracketClose = countquery.indexOf(">");
                    String orderCol = countquery.substring(bracketOpen + 1, bracketClose);
                    countquery = countquery.replace("<" + orderCol + ">", "");
                }
                ResultSet rcount = jdbcStmt.executeQuery(countquery);
                rcount.next();
                int count = Integer.parseInt(rcount.getArray(1).toString());

                //THIS IF NEEDS A WAY TO DETERMINE IF POSTGRES
                if (base.contains("postgres") && config.isPaginated) {
                    pages = (int) Math.ceil((double) count / config.pageSize);
                }
                // set up data in order
            }
            //establish new SourceReaders and DestinationWriters for pages
            SourceReader[] sr = new SourceReader[pages];
            DestinationWriter[] cr = new DestinationWriter[pages];
            for (int i = 0; i < pages; i++) {
                sr[i] = new SourceReader();
                cr[i] = new DestinationWriter();
            }
            Controller processor = new Controller<ArrayList<Object[]>>(client, sr, cr, query, key.toUpperCase() + ".insert", config, pages, base);
            completion.submit(processor);
        }

        // wait for all tasks to complete.
        for (int i = 0; i < keys.size(); ++i) {
            logger.info("****************" + completion.take().get() + " completed *****************"); // will block until the next sub task has completed.
        }

        executor.shutdown();
    }

    public static void main(String[] args) throws Exception {
        // process args
        Config cArgs = Config.getConfig("JdbcLoader", args);
        JdbcLoader loader = new JdbcLoader(cArgs);

        loader.logger.info("===============================================");
        loader.logger.info("This run of JDBC loader started at" + new Date());
        loader.logger.info("===============================================");

        loader.connectToSource();
        loader.client = VoltDBClientConnectionUtil.connectToVoltDB(cArgs);
        if (cArgs.queriesFile.isEmpty()) {
            loader.loadTables(cArgs.tables, cArgs.procname);
        } else {
            loader.load(cArgs.queriesFile, cArgs.modules.trim(), cArgs.tables.trim());
        }

        VoltDBClientConnectionUtil.close(loader.client);

        loader.logger.info("==================================================");
        loader.logger.info("This run of JDBC loader completed at" + new Date());
        loader.logger.info("==================================================");
        System.out.println("Read/write complete!");
    }

}
