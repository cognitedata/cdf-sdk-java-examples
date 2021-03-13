package com.cognite.sa.api;


/**
 * The schema of the request to the copy table endpoint. Client requests need to represent this
 * schema in the Json payload to the endpoint.
 */
public class CopyTableRequest {
    public String sourceDb;
    public String sourceTable;
    public String targetDb;
    public String targetTable;

    public CopyTableRequest(){}

    @Override
    public String toString() {
        return "CopyTableRequest{" +
                "sourceDb='" + sourceDb + '\'' +
                ", sourceTable='" + sourceTable + '\'' +
                ", targetDb='" + targetDb + '\'' +
                ", targetTable='" + targetTable + '\'' +
                '}';
    }
}
