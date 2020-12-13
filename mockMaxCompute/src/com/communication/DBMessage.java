package com.communication;

import java.util.Set;

public class DBMessage {
	@Override
	public String toString() {
		return "DBMessage [reqType=" + reqType + ", recordId=" + recordId + ", record=" + record + ", senderId="
				+ senderId + ", receiverId=" + receiverId + ", messageKey=" + messageKey + ", tableName=" + tableName
				+ ", tableNames=" + tableNames + ", startTime=" + startTime + ", endTime=" + endTime + "]";
	}

	// TODO: Add script to convert update and insert lines to SQL queries
	// Input: | separated entry
	// Output: SQL query
	private RequestType reqType;
	private String recordId;
	private String record;

	private String senderId = "";
	private String receiverId = "";
	private String messageKey = "";
	private String tableName;
	private Set<String> tableNames;
	private long startTime;
	private long endTime;

	// TODO do we need time stamp here?
	public DBMessage(RequestType reqType, String recordId, String record, String tableName, Set<String> tablesNamesSet) {
		this.recordId = recordId;
		this.record = record;
		this.reqType = reqType;
		this.tableName = tableName;
		this.tableNames = tablesNamesSet;
		// TODO add query as per TTPC_H
	}
	public DBMessage(RequestType reqType, String recordId, String record, String tableName) {
		this.recordId = recordId;
		this.record = record;
		this.reqType = reqType;
		this.tableName = tableName;
		//this.tableNames = tablesNamesSet;
		// TODO add query as per TTPC_H
	}

	public DBMessage() {

	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
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
		return this.messageKey != null ? this.messageKey : this.recordId + "_" + this.getTableName();
	}

	public void setMessageKey(String messageKey) {
		this.messageKey = messageKey;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public Set<String> getTableNames() {
		return tableNames;
	}

	public void setTableNames(Set<String> tableNames) {
		this.tableNames = tableNames;
	}
}
