package com.communication;


public class DBMessage {
	
	private RequestType reqType;
	private String recordId;
	private String record;
	

	public DBMessage() {}
	
	
	
	public DBMessage(RequestType read, String recordID, String record) {
		this.reqType = read;
		this.recordId = recordID;
		this.record = record;
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
