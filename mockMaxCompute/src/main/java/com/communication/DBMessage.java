package com.communication;


public class DBMessage {
	//TODO: Add script to convert update and insert lines to SQL queries
	//Input: | separated entry
	//Output: SQL query
	private RequestType reqType;
	private String recordId;
	private String record;


	public DBMessage() {}



	public DBMessage(RequestType reqType, String recordID, String record) {
		this.reqType = reqType;
		this.recordId = recordID;
		this.record = record;
		if (reqType==RequestType.READ){
			//return the message back as is
		}
		else{
			//parse and convert to query
		}
	}



	public RequestType getReqType() {
		return reqType;
	}
	public void setReqType(RequestType reqType) {
		this.reqType = reqType;
	}
	public String getRecordId() {
		return recordId;
	}
	public void setRecordId(String recordId) {
		this.recordId = recordId;
	}
	public String getRecord() {
		return record;
	}
	public void setRecord(String record) {
		this.record = record;
	}
}