package org.voltdb.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.VoltTable;
import org.voltdb.client.NoConnectionsException;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

/**
* Created by kunkunur on 3/12/14.
*/
class SourceReader extends Controller.Producer<ArrayList<Object[]>> {
    private final Logger logger;

    public SourceReader() {
        this.logger = LoggerFactory.getLogger(SourceReader.class);
    }

    protected void producerTask() {
        ArrayList<Object[]> arrayList = new ArrayList<Object[]>();
        try {
            if (config.srisvoltdb) {
//                System.out.println("SourceReader is VoltDB!");
//                System.out.println("Variable sez SR is:" + config.srisvoltdb);
//                System.out.println("Rows: "+ results.getRowCount());
                for (int j = 0; j < config.fetchsize; j++) {
                    results.advanceRow();
                    ArrayList<Object> processedResults = new ArrayList<Object>();
                    for (int k = 0; k < results.getColumnCount(); k++) {
                        Object result = results.get(k, results.getColumnType(k));
                        processedResults.add(result);
                    }
                    Object[] convertedResults = new Object[processedResults.size()];
                    processedResults.toArray(convertedResults);
                    arrayList.add(convertedResults);
//                    System.out.println("Current row: " + results.getActiveRowIndex());
                    if (results.getActiveRowIndex() >= results.getRowCount() - 1) {
                        System.out.println("Flying final producer flag for " + iteration + "! ArrayList length:" + arrayList.size());
                        endProducerFlag = true;
                    }
                }
                monitor.jobQueue.put(arrayList);
//                for (int j = 0; j < arrayList.size(); j++) {
//                    System.out.println("           Id: " + j + ", it: " + iteration);
//                }

                System.out.println("Pulled " + arrayList.size() + " records from the source! Active row: " + results.getActiveRowIndex() + ", iteration: " + iteration);
                logger.info("Pulled " + arrayList.size() + " records from the source");
                if (config.isPaginated) {
                    if ((results.getActiveRowIndex() + 1) % config.pageSize == 0 && results.getActiveRowIndex() != 0) {
                        System.out.println("Flying producer flag for " + iteration + "! ArrayList length:" + arrayList.size());
                        endProducerFlag = true;
                    }
                } else {
                    if (results.getActiveRowIndex() >= results.getRowCount() - 1) {
                        System.out.println("Flying producer flag for " + iteration + "! ArrayList length:" + arrayList.size());
                        endProducerFlag = true;
                    }
                }
            } else if (!config.srisvoltdb) {
//                System.out.println("SourceReader is not VoltDB!");
//                System.out.println("Variable sez SR is:" + config.srisvoltdb);
//                int records created to separate record counts of each producer, since they were interfering with each other
                int records = 0;
                while (rs.next()) {
                    monitor.records++;
                    records++;
                    // get one record of data as an Object[]
                    Object[] columnValues = new Object[columns];
                    for (int i = 0; i < columns; i++) {
                        columnValues[i] = rs.getObject(i + 1);
                    }

                    arrayList.add(columnValues);

                    if (records % config.fetchsize == 0) {
                        break;
                    }
                }
                monitor.jobQueue.put(arrayList);
//                System.out.println("Pulled " + arrayList.size() + " records from the source");
                logger.info("Pulled " + arrayList.size() + " records from the source");

                //Need that database type!
                if (controller.base.contains("postgres") && config.isPaginated) {
                    if ((rs.getRow() % config.pageSize == 0 && rs.getRow() != 0) || arrayList.size() == 0) {
                        System.out.println("Flying producer flag for " + iteration + "! ArrayList length:" + arrayList.size());
                        jdbcStmt.close();  //rs.close() happens implicitly when statement is closed.
                        endProducerFlag = true;
                    }
                } else {
                    if (arrayList.size() == 0) {
                        System.out.println("Flying producer flag for " + iteration + "! ArrayList length:" + arrayList.size());
                        jdbcStmt.close();  //rs.close() happens implicitly when statement is closed.
                        endProducerFlag = true;
                    }
                }
            }
                //this means all rows are read. This is the most cost effective solution instead of isLast  -
                //Refer : http://stackoverflow.com/questions/6722285/how-to-check-if-resultset-has-records-returned-w-o-moving-the-cursor-in-java
        } catch (SQLException e) {
            logger.info(Thread.currentThread().getName() + " - Exception occurred while executing the query: " + query);
            e.printStackTrace();
            controller.signalProducer(false);
        } catch (InterruptedException e) {
            logger.info(Thread.currentThread().getName() + " - Exception occurred while executing the query: " + query);
            e.printStackTrace();
            controller.signalProducer(false);
        } catch (Exception e) {
            logger.info(Thread.currentThread().getName() + " - unexpected exception occurred while executing the query: " + query);
            e.printStackTrace();
            controller.signal(false);
        }
    }
}
