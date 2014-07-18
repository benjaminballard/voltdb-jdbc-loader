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
    private JdbcLoader jdbcLoader;

    public DestinationWriter(JdbcLoader jdbcLoader) {
        this.jdbcLoader = jdbcLoader;
        this.logger = LoggerFactory.getLogger(Callback.class);

    }

    public void consumerTask() {
        // insert the record
        try {
            ArrayList<Object[]> arrayList = monitor.jobQueue.poll(jdbcLoader.config.maxWaitTime, TimeUnit.SECONDS);

            if (arrayList == null && !controller.signalProducer) {
                controller.signal(false);
            } else if (arrayList == null) {
                logger.info(Thread.currentThread().getName() + " waited for " + jdbcLoader.config.maxWaitTime + " seconds to retrieve new data.");
            } else {
                for (Object[] columnValues : arrayList) {
                    callback.setColumnValues(columnValues);

                    client.callProcedure(callback, voltProcedure, columnValues);
                }

                logger.info("Sent " + arrayList.size() + " requests to " + voltProcedure);
                callback.printProcedureIntermediateResults();
            }
        } catch (IOException e) {
            logger.error(Thread.currentThread().getName() + " - Exception occurred while invoking the procedure: " + voltProcedure + e.getMessage());
            e.printStackTrace();
            controller.signal(false);
        } catch (InterruptedException e) {
            logger.error(Thread.currentThread().getName() + " - Exception occurred while invoking the procedure: " + voltProcedure);
            logger.error("Interrupted while " + Thread.currentThread().getName() + " waited for " + jdbcLoader.config.maxWaitTime + " to retrieve new data. Hence ending the transfer now" + e.getMessage());
            e.printStackTrace();
            controller.signal(false);
        } catch (Exception e) {
            logger.error(Thread.currentThread().getName() + " - unexpected Exception occurred while invoking the procedure: " + voltProcedure + e.getMessage());
            e.printStackTrace();
            controller.signal(false);
        }
    }
}
