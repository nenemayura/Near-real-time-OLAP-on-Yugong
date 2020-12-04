package com.stateTable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.constants.DatabaseConstants;

public class StateTableOperationManager {
	
	
	private Connection dbConnection;
	
	public StateTableOperationManager() {}
	
	public StateTableOperationManager(final Connection dbConn)  {
		this.dbConnection = dbConn;
	}
	
	// insert
	
	public void insertIntoStateTable(final String recordId, final String tableName, final List<Integer> nodeIdsList) {
		
		final String nodeIdString = getConcatenatedString(nodeIdsList);
		final StringBuffer query = new StringBuffer();
		int rs = 0;
		query.append("INSERT INTO ");
		query.append(DatabaseConstants.STATE_TABLE); 
		query.append(" VALUES ( ");
		query.append(recordId);
		query.append(DatabaseConstants.COMMA_DELIMETER);
		query.append("\"");
		query.append(tableName);
		query.append("\"");
		query.append(DatabaseConstants.COMMA_DELIMETER);
		query.append("\"");
		query.append(nodeIdString);
		query.append("\"");
		query.append(" )");
		System.out.println("Query is "+query);
		try {
			final Statement stmt = this.dbConnection.createStatement();
			rs = stmt.executeUpdate(query.toString());
			System.out.println("Result is "+rs);

		} catch (SQLException e) {
			System.out.println("Error"+ e +" while inserting record "+recordId);
		}
		
	}

	// update
	public void updateStateTable(final String recordId, final String tableName, final List<Integer> nodeIdsList) {
	
		final String nodeIdString = getConcatenatedString(nodeIdsList);
		int rs = 0;
		final StringBuffer query = new StringBuffer();
		query.append("UPDATE ");
		query.append(DatabaseConstants.STATE_TABLE);
		query.append(DatabaseConstants.SPACE_DELIMETER);
		query.append("SET "); 
		query.append(DatabaseConstants.NODES_COLUMN_NAME);
		query.append(" = \"");
		query.append(nodeIdString);
		query.append("\" WHERE id = ");
		query.append(recordId);
		query.append(" AND ");
		query.append(DatabaseConstants.TABLENAME_COLUMN_NAME);
		query.append(" = ");
		query.append("\"");
		query.append(tableName);
		query.append("\"");
		System.out.println("Query is "+query);
		try {
			final Statement stmt = this.dbConnection.createStatement();
			rs = stmt.executeUpdate(query.toString());
			
			System.out.println("Result is "+rs);
			
		} catch (SQLException e) {
			System.out.println("Error"+ e +" while updating record "+recordId);
		}
	}
	
	// read
	public String readFromStateTable(final String recordId, final String tableName) {
		String result = "";
		ResultSet rs = null;
		final StringBuffer query = new StringBuffer();
		query.append("SELECT * FROM ");
		query.append(DatabaseConstants.STATE_TABLE);
		query.append(DatabaseConstants.SPACE_DELIMETER);
		query.append(" WHERE id = ");
		query.append(recordId);
		query.append(" AND ");
		query.append(DatabaseConstants.TABLENAME_COLUMN_NAME);
		query.append(" = ");
		query.append("\"");
		query.append(tableName);
		query.append("\"");
		
		try {
			final Statement stmt = this.dbConnection.createStatement();
			rs = stmt.executeQuery(query.toString());
			while(rs.next()) {
				result = rs.getString(DatabaseConstants.NODES_COLUMN_NAME);
			}
		} catch (SQLException e) {
			System.out.println("Error"+ e +" while selecting record "+recordId);
		}
		
		return result;
	}
	
	
	private String getConcatenatedString(final List<Integer> nodeIdsList) {
		
		final StringBuffer nodes = new StringBuffer();
		
		nodeIdsList.forEach(nodeId -> {
			nodes.append(nodeId);
			nodes.append(DatabaseConstants.COMMA_DELIMETER);
		});
		nodes.deleteCharAt(nodes.length()-1);
		return nodes.toString();
	}
	
	
	

}
