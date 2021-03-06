package com.communication;
public class RequestStat {

	long startTime = 0;
	long endTime = 0;
	String requestType;
	boolean processed = false;
	int readQ = 0;
	int writeQ = 0;
	int numNodes = 0;
	float inConsistencyCount = 0.0f;
	public RequestStat(long startTime, long endTime, String requestType, boolean processed, int readQ, int writeQ,
			int numNodes) {
		super();
		this.startTime = startTime;
		this.endTime = endTime;
		this.requestType = requestType;
		this.processed = processed;
		this.readQ = readQ;
		this.writeQ = writeQ;
		this.numNodes = numNodes;
	}
	
	public RequestStat(long startTime, long endTime, String requestType, boolean processed, 
			int numNodes, float inConsistencyCount) {
		super();
		this.startTime = startTime;
		this.endTime = endTime;
		this.requestType = requestType;
		this.processed = processed;
		this.numNodes = numNodes;
		this.inConsistencyCount = inConsistencyCount;
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

	public String getRequestType() {
		return requestType;
	}

	public void setRequestType(String requestType) {
		this.requestType = requestType;
	}

	public boolean isProcessed() {
		return processed;
	}

	public void setProcessed(boolean processed) {
		this.processed = processed;
	}

	public int getReadQ() {
		return readQ;
	}

	public void setReadQ(int readQ) {
		this.readQ = readQ;
	}

	public int getWriteQ() {
		return writeQ;
	}

	public void setWriteQ(int writeQ) {
		this.writeQ = writeQ;
	}

	public int getNumNodes() {
		return numNodes;
	}

	public void setNumNodes(int numNodes) {
		this.numNodes = numNodes;
	}

}
