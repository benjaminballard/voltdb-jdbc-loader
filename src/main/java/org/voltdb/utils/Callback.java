package org.voltdb.utils;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

public class Callback implements ProcedureCallback {

    private static Multiset<String> stats = ConcurrentHashMultiset.create();
    private final Logger logger;
    private Object[] columnValues;
    String procedureName;
    long maxErrors;
    Controller controller;

    public static int count(String procedureName, String event) {
        return stats.add(procedureName + event, 1);
    }

    public static int getCount(String procedureName, String event) {
        return stats.count(procedureName + event);
    }

    public Callback(String procedure, long maxErrors, Controller controller) {
        super();
        this.procedureName = procedure;
        this.maxErrors = maxErrors;
        this.controller = controller;
        logger = LoggerFactory.getLogger(Callback.class);
    }

    public void printProcedureResults(String procedureName) {
        logger.info("------------------");
        logger.info("  " + procedureName);
        logger.info("        calls: " + getCount(procedureName, "call"));
        logger.info("      commits: " + getCount(procedureName, "commit"));
        logger.info("    rollbacks: " + getCount(procedureName, "rollback"));
        logger.info("------------------");
    }

    public void printProcedureIntermediateResults() {
        logger.info(procedureName + ": total commits thus far: " + getCount(procedureName, "commit"));
    }

    public void setColumnValues(Object[] columnValues) {
        this.columnValues = columnValues;
    }

    @Override
    public void clientCallback(ClientResponse cr) {
        count(procedureName, "call");

        if (cr.getStatus() == ClientResponse.SUCCESS) {
            count(procedureName, "commit");
        } else {
            long totalErrors = count(procedureName, "rollback");

            logger.info("Column values were:");

            for (Object value : columnValues) {
                logger.info(String.valueOf(value));
            }

            logger.info("DATABASE ERROR Status: " + cr.getStatusString());

            if (totalErrors > maxErrors) {
                logger.error("exceeded " + maxErrors + " permissible per table. Stopping data load for this table-" + procedureName);

                printProcedureResults(procedureName);

                controller.signal(false);
            }
        }
    }
}

