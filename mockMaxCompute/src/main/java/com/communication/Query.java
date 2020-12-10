package com.communication;

public class QueryProcessing {
    private RequestType reqType;
    private String message;
    private String query;

    public QueryProcessing() {}



    public QueryProcessing(RequestType reqType, String message ) {
        this.reqType = reqType;
        this.message = message;
        if (reqType==RequestType.READ){
            //return the message back as is
            setQuery(message);
        }
        else {
            //parse and convert to query
            setQuery(convertToQuery(reqType,message));

        }
    }

    public static String convertToQuery(RequestType reqType, String values){
        String query=null;
        if (reqType == RequestType.EDIT){
            String[] sepValues = values.split("|");
            query = "UPDATE ";
        }
        else if (reqType == RequestType.INSERT){
            String commaSepValues = values.replace("|",", ");
            query = "INSERT INTO orders VALUES ("+commaSepValues+");";

        }
        if (query == null){
            throw new IllegalArgumentException();
        }
        return query;
    }

    public RequestType getReqType() {
        return reqType;
    }
    public void setReqType(RequestType reqType) {
        this.reqType = reqType;
    }
    public String getMessage() {
        return message;
    }
    public void setRecordId(String message) {
        this.message = message;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}


