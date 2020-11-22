
public class DBMessage {
	
	private RequestType reqType;
	private String recordId;
	private String record;
	
	//TODO do we need time stamp here?
	public DBMessage(RequestType reqType, String recordId, String record) {
		this.recordId = recordId;
		this.record = record;
		this.reqType = reqType;
	}
	public DBMessage() {
	
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
