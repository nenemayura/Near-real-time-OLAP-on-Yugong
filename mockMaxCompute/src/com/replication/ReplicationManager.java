/**
 * 
 */
package com.replication;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.communication.TableName;
import com.constants.DatabaseConstants;

/**
 * @author nenem
 *
 */
public class ReplicationManager {

	private List<String> replicatedTables;
	private Connection localConnection;
	/**
	 * @param args - IP of the remote DB and tables to be copied
	 */

// TODO Add inout arguments IP and table name to be copied
	//public static void main(String[] args) {

		// get local connection
	public ReplicationManager() {
		this.replicatedTables = new ArrayList();
		this.localConnection = DatabaseConnection.getConnection();

	}
		
		
		// get remote connection
//		Connection remoteConnection = getRemoteDatabaseConnection();
//		
//		// select from local
//		
//		ResultSet rs = selectDataFromLocalDatabase(localConnection);
//		

//		// insert into remote
//		replicateDataToRemote(remoteConnection, rs, tableName);
//
//		//delete from local
//		deleteReplicatedTables();

	//}
	public ResultSet selectDataFromRemote(Connection remoteConnection, String tableName){
		Statement statement;
		ResultSet rs = null;
		try{
			statement = remoteConnection.createStatement();
			rs = statement.executeQuery("SELECT * FROM "+tableName);

		}
		catch (Exception e){
			System.out.println("Error while executing statement "+e);
		}
		return rs;
	}
	public void replicateDataFromRemote(ResultSet rs, String tableName){

		try{
			System.out.println("Replicating data from remote");
			ResultSetMetaData meta = rs.getMetaData();
			List<String> columns = new ArrayList<>();
			for (int i = 1; i <= meta.getColumnCount(); i++)
				columns.add(meta.getColumnName(i));

			PreparedStatement replicationQuery = this.localConnection.prepareStatement(
					"INSERT INTO " + tableName + " ("
							+ columns.stream().collect(Collectors.joining(", "))
							+ ") VALUES ("
							+ columns.stream().map(c -> "?").collect(Collectors.joining(", "))
							+ ")");

			while (rs.next()) {
				for (int i = 1; i <= meta.getColumnCount(); i++)
					replicationQuery.setObject(i, rs.getObject(i));

				replicationQuery.addBatch();
			}
			replicationQuery.executeBatch();
			this.replicatedTables.add(tableName);
			System.out.println("Data replicated");

		} catch (SQLException e) {
			System.out.println("Error while executing statement "+e);
		}

	}
	public  void replicateDataToRemote(Connection remoteConnection, ResultSet rs, String tableName) {
		try {
			System.out.println("Replicating data to remote");
			ResultSetMetaData meta = rs.getMetaData();
			 List<String> columns = new ArrayList<>();
		     for (int i = 1; i <= meta.getColumnCount(); i++)
		         columns.add(meta.getColumnName(i));

		     PreparedStatement replicationQuery = remoteConnection.prepareStatement(
		                "INSERT INTO " + tableName + " ("
		              + columns.stream().collect(Collectors.joining(", "))
		              + ") VALUES ("
		              + columns.stream().map(c -> "?").collect(Collectors.joining(", "))
		              + ")");

		    while (rs.next()) {
		        for (int i = 1; i <= meta.getColumnCount(); i++)
		            replicationQuery.setObject(i, rs.getObject(i));

		        replicationQuery.addBatch();
		        }
		   replicationQuery.executeBatch();
			this.replicatedTables.add(tableName);
		   System.out.println("Data replicated");

		} catch (SQLException e) {
			System.out.println("Error while executing statement "+e);
		}

	}
	

	private static ResultSet selectDataFromLocalDatabase(final Connection localConnection) {
		
		Statement stmt;
		ResultSet rs = null;
		try {
			stmt = localConnection.createStatement();
			rs =stmt.executeQuery("select * from emp"); 

		} catch (SQLException e) {
			System.out.println("Error while executing statement "+e);
		}
		return rs;  
	}

	public  Connection getRemoteDatabaseConnection(String remoteIp) {
		
		String remoteUrl = "jdbc:mysql://"+remoteIp+":3306/tpch?characterEncoding=latin1";
		System.out.println("Connecting remote database...");
		Connection remoteConnection = null;
		try { 
			Class.forName("com.mysql.jdbc.Driver"); 
			remoteConnection = DriverManager.getConnection(remoteUrl, DatabaseConstants.USERNAME, 
				DatabaseConstants.PASSWORD);
		    System.out.println("Remote Database connected!");
		} catch (SQLException e) {
			//e.printStackTrace();
			System.out.println("Error while connecting to remote database "+e);
		} catch (ClassNotFoundException e) {
			//e.printStackTrace();
			System.out.println("Error while connecting to remote database "+e);

		} 
		return remoteConnection;
	}

	public void deleteReplicatedTables(List<String>replicatedTables ){
		System.out.println("Deleting replicated tables");
		for (String t:replicatedTables){
			String deleteQuery = "DROP TABLE "+t+";";
			try {
				Statement stmt = this.localConnection.createStatement();
				ResultSet rs = stmt.executeQuery(deleteQuery);
				System.out.println("Successfully deleted table "+t);

			} catch (SQLException e) {
				System.out.println("Error while executing statement "+e);
			}

		}
	}
	public List<String> getReplicatedTables() {
		return replicatedTables;
	}

	public void setReplicatedTables(List<String> replicatedTables) {
		this.replicatedTables = replicatedTables;
	}

}
