package com.dbOperations;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.communication.DBMessage;
import com.communication.RequestType;

import replication.test.DatabaseConnection;


public class DBOperationManager {
	final Connection localDbConnection;

	public DBOperationManager() {
		localDbConnection = DatabaseConnection.getConnection();
	}
	
	public String processMessageRequest(final DBMessage messageReceived) {
	
		// switch case 
		final String result =  processRequestType(messageReceived);
		
		return result;
	}
	
	private String processRequestType(final DBMessage messageReceived) {
		
		// parse the message
		final RequestType requestType = messageReceived.getReqType();
		String result = "";
		switch(requestType) {
			case INSERT:
				processInsertRequest(messageReceived);
				break;
			case EDIT:
				processEditRequest(messageReceived);
				break;
			case DELETE: 
				processDeleteRequest(messageReceived);
				break;
			case READ:
				result = processReadRequest(messageReceived);
				break;
			default:
				System.out.println("Requested operation not available");
				break;
		}
		return result;
				
	}
	private String processReadRequest(DBMessage messageReceived) {
		// record value will be ID, tablename in future
		final String recordValues = messageReceived.getRecordId();
		ResultSet rs = null;
		String result = "";
		final StringBuffer query = new StringBuffer();
		query.append("SELECT * FROM ");
		query.append("emp ");
		query.append(" WHERE id = ");
		query.append(recordValues);
		
		try {
			final Statement stmt = localDbConnection.createStatement();
			rs = stmt.executeQuery(query.toString());
			while(rs.next()) {
				result = rs.getString("Name");
			}
		} catch (SQLException e) {
			System.out.println("Error"+ e +" while selecting record "+recordValues);
		}
		
		return result;
	}

	private void processDeleteRequest(DBMessage messageReceived) {
		// record ID  to delete will be in message for ex: 2, tablename in future
		final String recordValues = messageReceived.getRecordId();
		final StringBuffer query = new StringBuffer();
		int rs = 0;
		query.append("DELETE from emp ");
		query.append("WHERE id = ");
		query.append(recordValues);
		try {
			final Statement stmt = localDbConnection.createStatement();
			rs = stmt.executeUpdate(query.toString());
			System.out.println("Result is "+rs);
		} catch (SQLException e) {
			System.out.println("Error"+ e +" while deleting record "+recordValues);
		}
		
	}

	private void processEditRequest(DBMessage messageReceived) {
		// record ID and string to insert will be in message for ex: 2, name, "ChangedName" tablename in future
		final String recordValues = messageReceived.getRecord();
		final String[] splitRecords = recordValues.split(",");
		int rs = 0;
		if(splitRecords.length > 0) {
			final String columnName = splitRecords[0];
			final String columnValue = splitRecords[1];
			final StringBuffer query = new StringBuffer();
			query.append("UPDATE emp ");
			query.append("SET "); 
			query.append(columnName);
			query.append(" = \"");
			query.append(columnValue);
			query.append("\" WHERE id = ");
			query.append(messageReceived.getRecordId());
			System.out.println("Query is "+query);
			try {
				final Statement stmt = localDbConnection.createStatement();
				rs = stmt.executeUpdate(query.toString());
				
				System.out.println("Result is "+rs);
				
			} catch (SQLException e) {
				System.out.println("Error"+ e +" while updating record "+recordValues);
			}
		} else {
			System.out.println("Error"+ "Empty message" +" while updating record "+recordValues);

		}

		
	}

	private void processInsertRequest(final DBMessage messageReceived) {
		
		final String recordValues = messageReceived.getRecord();
		final StringBuffer query = new StringBuffer();
		int rs = 0;
		query.append("INSERT INTO ");
		query.append("emp"); //get table name also from the message
		query.append(" VALUES ( ");
		query.append(messageReceived.getRecordId());
		query.append(",");
		query.append(recordValues);
		query.append(" )");
		System.out.println("Query is "+query);
		try {
			final Statement stmt = localDbConnection.createStatement();
			rs = stmt.executeUpdate(query.toString());
			System.out.println("Result is "+rs);

		} catch (SQLException e) {
			System.out.println("Error"+ e +" while inserting record "+recordValues);
		}
		
		
	}
}
