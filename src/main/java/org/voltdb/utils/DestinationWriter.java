package org.voltdb.utils;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
* Created by kunkunur on 3/12/14.
*/
class DestinationWriter extends Controller.Consumer<ArrayList<Object[]>> {
    private final Logger logger;

    public DestinationWriter() {
        this.logger = LoggerFactory.getLogger(Callback.class);
    }

    public void consumerTask() {
        // insert the record
        try {
            ArrayList<Object[]> arrayList = monitor.jobQueue.poll(config.maxWaitTime, TimeUnit.SECONDS);

            if (config.dwisvoltdb) {
//                System.out.println("DestinationWriter is VoltDB!");
//                System.out.println("Variable sez DR is:" + config.dwisvoltdb);
                if (arrayList == null && !controller.signalProducer) {
                    controller.signal(false);
                } else if (arrayList == null) {
                    logger.info(Thread.currentThread().getName() + " waited for " + config.maxWaitTime + " seconds to retrieve new data.");
                } else {
                    for (Object[] columnValues : arrayList) {
                        callback.setColumnValues(columnValues);

                        client.callProcedure(callback, procName, columnValues);
                        tasks++;
                    }

                    logger.info("Sent " + arrayList.size() + " requests to " + procName);
                    callback.printProcedureIntermediateResults();
                    if (controller.base.contains("postgres") && config.isPaginated) {
                        if (tasks % config.pageSize == 0 && tasks != 0) {
                            System.out.println("Flying consumer flag for " + iteration + "!");
                            endConsumerFlag = true;
                        }
                    }
                }
            } else if (!config.dwisvoltdb) {
//                System.out.println("DestinationWriter is not VoltDB!");
//                System.out.println("Variable sez DR is:" + config.dwisvoltdb);
                if (arrayList == null && !controller.signalProducer) {
                    controller.signal(false);
                } else if (arrayList == null) {
                    logger.info(Thread.currentThread().getName() + " waited for " + config.maxWaitTime + " seconds to retrieve new data.");
                } else {
                    for (Object[] columnValues : arrayList) {
                        tasks++;
                        StringBuilder valueSB = new StringBuilder();
                        for (int i = 0; i < columnValues.length; i++) {
                            if (i != 0) {
                                valueSB.append(", ");
                            }
//                            System.out.println(columnValues[i]);
                            valueSB.append("'").append(columnValues[i]).append("'");
                        }
                        String sql = sb.toString()
                                + "VALUES (" + valueSB.toString() + "))";
//                        System.out.println(sql);
                        try {
                            jdbcStmt.executeUpdate(sql);
                            if (tasks >= controller.producers[iteration].results.getRowCount()) {
                                System.out.println("Flying final consumer flag for " + iteration + "!");
                                endConsumerFlag = true;
                            }
                        } catch (Exception e) {
//                            System.out.println("Duplicate!");
                            continue;
                        }
                    }
                    if (config.isPaginated) {
                        if (tasks % config.pageSize == 0 && tasks != 0) {
                            System.out.println("Flying consumer flag for " + iteration + "!");
                            endConsumerFlag = true;
                        }
                    }
                }
            }
        } catch (IOException e) {
            logger.error(Thread.currentThread().getName() + " - Exception occurred while invoking the procedure: " + procName + e.getMessage());
            e.printStackTrace();
            controller.signal(false);
        } catch (InterruptedException e) {
            logger.error(Thread.currentThread().getName() + " - Exception occurred while invoking the procedure: " + procName);
            logger.error("Interrupted while " + Thread.currentThread().getName() + " waited for " + config.maxWaitTime + " to retrieve new data. Hence ending the transfer now" + e.getMessage());
            e.printStackTrace();
            controller.signal(false);
        } catch (Exception e) {
            logger.error(Thread.currentThread().getName() + " - unexpected Exception occurred while invoking the procedure: " + procName + e.getMessage());
            e.printStackTrace();
            controller.signal(false);
        }
    }
}
