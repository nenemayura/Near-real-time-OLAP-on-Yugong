package com.communication;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Query {
    private RequestType reqType;
    private String message;
    private TableName tableName;
    private String query;


    public Query(RequestType reqType, String message){
        this.reqType = reqType;
        this.message = message;
        if (reqType!=RequestType.READ){
            throw new IllegalArgumentException("Too few arguments for non-read query");
        }
        setQuery(message);

    }
    public Query(RequestType reqType, String message, TableName tableName ) {
        this.reqType = reqType;
        this.message = message;
        this.tableName=tableName;
        try{
            setQuery(convertToQuery(reqType,message,tableName));
        }
        catch (Exception e){
            e.printStackTrace();
            throw e;
        }
    }

    public String convertToQuery(RequestType reqType, String values, TableName tableName){
        String query="";
        query = "UPDATE orders SET ";
        if (reqType == RequestType.EDIT){
            List<String> cols = Arrays.asList("O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE","O_ORDERDATE","O_ORDERPRIORITY","O_CLERK","O_SHIPPRIORITY","O_COMMENT");
            String[] sepVals = values.split("|");
            for (int i=0;i<cols.size();i++){
                if (i>0)
                    query = query+ ", "+ cols.get(i) + " = "+ sepVals[i];
                else
                    query = query+ cols.get(i) + " = "+ sepVals[i];

            }

        }
        else if (reqType == RequestType.INSERT){
            String commaSepValues = values.replace("|",", ");
            query = "INSERT INTO " + tableName.name() + " VALUES ("+commaSepValues+");";

        }
        else if (reqType == RequestType.READ){
            return values;
        }
        if (query == null){
            throw new IllegalArgumentException("Invalid request type for convertToQuery");
        }
        return query;
    }

    public RequestType getReqType() {
        return reqType;
    }
    public void setReqType(RequestType reqType) {
        this.reqType = reqType;
    }



    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public TableName getTableName() {
        return tableName;
    }

    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }
}


