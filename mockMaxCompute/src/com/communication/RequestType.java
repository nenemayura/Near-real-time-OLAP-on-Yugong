package com.communication;

public enum RequestType {
	INSERT,
	EDIT,
	DELETE,
	READ,
	ACK_INSERT,
	ACK_EDIT,
	ACK_DELETE,
	READ_RESPONSE,
	TPC_READ,
	REP_TABLES,//If the message from sub contains Set<TableNames> replicated 
	CONSISTENCY_CHECK, 
	ACK_CONSISTENCY_CHECK
}
