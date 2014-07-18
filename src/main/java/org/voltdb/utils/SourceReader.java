package org.voltdb.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;

/**
* Created by kunkunur on 3/12/14.
*/
class SourceReader extends Controller.Producer<ArrayList<Object[]>> {
    private final Logger logger;
    private JdbcLoader jdbcLoader;

    public SourceReader(JdbcLoader jdbcLoader) {
        this.jdbcLoader = jdbcLoader;
        this.logger = LoggerFactory.getLogger(SourceReader.class);
    }

    protected void producerTask() {
        ArrayList<Object[]> arrayList = new ArrayList<Object[]>();

        try {
            while (rs.next()) {
                monitor.records++;

                // get one record of data as an Object[]
                Object[] columnValues = new Object[columns];
                for (int i = 0; i < columns; i++) {
                    columnValues[i] = rs.getObject(i + 1);
                }

                arrayList.add(columnValues);

                if (monitor.records % jdbcLoader.config.fetchsize == 0) {
                    break;
                }
            }

            //this means all rows are read. This is the most cost effective solution instead of isLast  -
            //Refer : http://stackoverflow.com/questions/6722285/how-to-check-if-resultset-has-records-returned-w-o-moving-the-cursor-in-java
            if (arrayList.size() == 0) {
                jdbcStmt.close();  //rs.close() happens implicitly when statement is closed.
                controller.signalProducer(false);
            } else {
                monitor.jobQueue.put(arrayList);

                logger.info("Pulled " + arrayList.size() + " records from the source");
            }
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
