/**
 * 
 */
package replication.test;

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

import com.constants.DatabaseConstants;

/**
 * @author nenem
 *
 */
public class ReplicationManager {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// get local connection
		
		Connection localConnection = DatabaseConnection.getConnection();
		
		// get remote connection
		Connection remoteConnection = getRemoteDatabaseConnection();
		
		// select from local
		
		ResultSet rs = selectDataFromLocalDatabase(localConnection);
		
		// insert into remote
		replicateDataToRemote(remoteConnection, rs);
	}

	private static void replicateDataToRemote(Connection remoteConnection, ResultSet rs) {
		try {
			System.out.println("Replicating data to remote");
			ResultSetMetaData meta = rs.getMetaData();
			 List<String> columns = new ArrayList<>();
		     for (int i = 1; i <= meta.getColumnCount(); i++)
		         columns.add(meta.getColumnName(i));

		     PreparedStatement replicationQuery = remoteConnection.prepareStatement(
		                "INSERT INTO " + "emp" + " ("
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

	private static Connection getRemoteDatabaseConnection() {
		
		String remoteUrl = "jdbc:mysql://10.32.102.43:3306/testdatabase?characterEncoding=latin1";
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

}
