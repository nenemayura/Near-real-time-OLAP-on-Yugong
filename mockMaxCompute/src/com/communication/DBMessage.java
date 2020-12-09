package com.communication;

public class DBMessage {
	
	private RequestType reqType;
	private String recordId;
	private String record;
	private String senderId= "";
	private String receiverId= "";
	private String messageKey = "";
	
	//TODO do we need time stamp here?
	public DBMessage(RequestType reqType, String recordId, String record) {
		this.recordId = recordId;
		this.record = record;
		this.reqType = reqType;
		//TODO add query as per TTPC_H
	}
	public DBMessage() {
	
	}
	public String getSenderId() {
		return senderId;
	}
	public void setSenderId(String senderId) {
		this.senderId = senderId;
	}
	public String getReceiverId() {
		return receiverId;
	}
	public void setReceiverId(String receiverId) {
		this.receiverId = receiverId;
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
	public String getMessageKey() {
		return this.reqType+"_"+ this.recordId+"_";
		
	}
	public void setMessageKey(String messageKey) {
		this.messageKey = this.reqType+"_"+ this.recordId+"_";
		
	}
}
