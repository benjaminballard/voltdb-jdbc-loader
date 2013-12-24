package org.voltdb.utils;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

public class LoaderCallback implements ProcedureCallback {

    private static Multiset<String> stats = ConcurrentHashMultiset.create();
    private final Object[] columnValues;
    String procedureName;
    long maxErrors;

    public static int count(String procedureName, String event) {
        return stats.add(procedureName + event, 1);
    }

    public static int getCount(String procedureName, String event) {
        return stats.count(procedureName + event);
    }

    public static void printProcedureResults(String procedureName) {
        System.out.println("  " + procedureName);
        System.out.println("        calls: " + getCount(procedureName, "call"));
        System.out.println("      commits: " + getCount(procedureName, "commit"));
        System.out.println("    rollbacks: " + getCount(procedureName, "rollback"));
    }

    public LoaderCallback(String procedure, long maxErrors, Object[] columnValues) {
        super();
        this.procedureName = procedure;
        this.maxErrors = maxErrors;
        this.columnValues =  columnValues;
    }

    public LoaderCallback(String procedure) {
        this(procedure, 5l,null);
    }

    @Override
    public void clientCallback(ClientResponse cr) {

        count(procedureName, "call");

        if (cr.getStatus() == ClientResponse.SUCCESS) {
            count(procedureName, "commit");
        } else {
            long totalErrors = count(procedureName, "rollback");


            System.out.println("Column values were:");

            for(Object value : columnValues){
                System.out.println(String.valueOf(value));
            }

            //cr.getException().printStackTrace();

            System.out.println("DATABASE ERROR Status: " + cr.getStatusString());

            if (totalErrors > maxErrors) {
                System.err.println("exceeded " + maxErrors + " maximum database errors - exiting client");
                printProcedureResults(procedureName);
                System.exit(-1);
            }

        }
    }
}

