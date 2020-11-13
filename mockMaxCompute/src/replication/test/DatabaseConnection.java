
package replication.test;

/**
 * @author nenem
 *
 */

  
import java.sql.Connection; 
import java.sql.DriverManager; 
import java.sql.SQLException;

import com.constants.DatabaseConstants; 
  
public class DatabaseConnection { 
  
    private static Connection con = null; 
  
    static
    { 
        String url = "jdbc:mysql://localhost:3306/testdatabase?characterEncoding=latin1"; 

		System.out.println("Connecting database...");

        try { 
            Class.forName("com.mysql.jdbc.Driver"); 
            con = DriverManager.getConnection(url, DatabaseConstants.USERNAME, DatabaseConstants.PASSWORD); 
		    System.out.println("Database connected!");
        } 
        catch (ClassNotFoundException | SQLException e) { 
            e.printStackTrace(); 
        } 
    } 
    public static Connection getConnection() 
    { 
        return con; 
    } 
} 
