package main.java.com.communication;

import com.communication.DBMessage;
import com.communication.Query;
import com.communication.RequestType;
import com.communication.TableName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.xml.internal.fastinfoset.util.StringArray;


import javax.management.Query;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;


public class DBMessageSource {
	public static ServerSocket sourceToPubSocket;
    static String messageSourceIp = "localhost";
    static int messageSourcePort = 6432;
    static String path = "/home/student/code/TPC_H_2_18_0_rc2/dbgen/";

   
    // give args as ip, port 
   
	public static void main(String[] args) {
		if(args.length >0 ) {
			if(args[0]!= null) {
				messageSourceIp = args[0];
			}
			if(args[1] !=null) {
				messageSourcePort = Integer.valueOf(args[1]);
			}
		}
		sendMessages();
	}
    public static void sendMessages() {

		Thread send = new Thread() {
			public void run() {

				Socket publisherSocket = null;
				try {
					sourceToPubSocket = new ServerSocket(messageSourcePort);
					publisherSocket = sourceToPubSocket.accept();

				} catch (IOException e1) {
					System.out.println("exception "+e1.getMessage());
				}
				System.out.println("Starting thread at message source to send messages to publisher");
				//while (true) {
					try {
        				ObjectMapper objMapper = new ObjectMapper();
						DataOutputStream dos = new DataOutputStream(publisherSocket.getOutputStream());
						Thread.sleep(10000);
        				
//        				DBMessage message = new DBMessage(RequestType.READ, "1", "");
//						dos.writeUTF(objMapper.writeValueAsString(message));
//						System.out.println("Message sent from source to publisher:"+ objMapper.writeValueAsString(message));
						//Read 10 times and write/update once

						try{
							Scanner readQueryScanner;
							File readQueriesFile;
							Scanner updateQueryScanner;
							Scanner insertQueryScanner;

							try{
								readQueriesFile = new File(path+"tpch-stream.sql");
								readQueryScanner = new Scanner(readQueriesFile).useDelimiter(";");
							}
							catch (FileNotFoundException e) {
								System.out.println("An error occurred.");
								e.printStackTrace();
								throw new FileNotFoundException();

							}
//							try{
//								updateQueryScanner = new Scanner(new File("update.tbl.u1"));
//							}
//							catch (FileNotFoundException e) {
//								System.out.println("An error occurred.");
//								e.printStackTrace();
//								throw new FileNotFoundException();
//							}
//							try{
//								insertQueryScanner = new Scanner(new File("insert.tbl"));
//							}
//							catch (FileNotFoundException e) {
//								System.out.println("An error occurred.");
//								e.printStackTrace();
//
//								throw new FileNotFoundException();
//							}

//							int readOffset = 0;
//							int temp = 0;


							while(true) {

								while (readQueryScanner.hasNext() && readOffset < 10) {

									Query readQuery = new Query(RequestType.READ, readQueryScanner.next() + ";");
									//System.out.println(readQuery.getQuery());
									String queryString = readQuery.getQuery();
									if (!queryString.equals("\n;")) {
										Set<String> tableNames = getTableNames(queryString);
										DBMessage message = new DBMessage(RequestType.TPC_READ, "1", readQuery.getQuery(), "", tableNames);
										if (temp == 4) {
											dos.writeUTF(objMapper.writeValueAsString(message));

										}
										System.out.println("Message sent from source to publisher:" + objMapper.writeValueAsString(message));
										System.out.println("---------------------------");
										Thread.sleep(1000);

										readOffset++;
										temp++;
									}
								}


								if (readOffset == 10)
									readOffset = 0;
								if (!readQueryScanner.hasNext()) {
									readQueryScanner = new Scanner(readQueriesFile).useDelimiter(";");
									break;
								}

								// adding consistency query

//								String consistencyQuery = "Select SUM(totalprice) as consistencySum from orders;";
//								DBMessage message = new DBMessage(RequestType.CONSISTENCY_CHECK, "1", consistencyQuery, "");
//								dos.writeUTF(objMapper.writeValueAsString(message));
//								System.out.println("Message sent from source to publisher: for consistency query"+ objMapper.writeValueAsString(message));
//								


								if (updateQueryScanner.hasNextLine()) {
									Query updateQuery = new Query(RequestType.EDIT, updateQueryScanner.nextLine(), TableName.customer);
									String queryString = updateQuery.getQuery();
									if (!queryString.equals("\n;")) {
//										Set<String> tableNames = getTableNames(queryString);

										DBMessage message = new DBMessage(RequestType.EDIT, "1", queryString);
										System.out.println("Message sent from source to publisher:" + objMapper.writeValueAsString(message));
										System.out.println("---------------------------");
									}

//								else
//									break;
//								if (insertQueryScanner.hasNextLine()){
//									Query insertQuery = new Query(RequestType.INSERT,insertQueryScanner.nextLine(),TableName.customer);
//									System.out.println(insertQuery.getQuery());
//									Thread.sleep(10000);
//
//								}
//								else
//									break;


								}

								String consistencyQuery = "Select SUM(O_TOTALPRICE) as consistencySum from orders;";
								DBMessage message = new DBMessage(RequestType.CONSISTENCY_CHECK, "1", consistencyQuery, "");
								dos.writeUTF(objMapper.writeValueAsString(message));
								System.out.println("Message sent from source to publisher: for consistency query" + objMapper.writeValueAsString(message));

								Thread.sleep(10000);
							}
				

							readQueryScanner.close();
							updateQueryScanner.close();
							insertQueryScanner.close();
						}
						catch (FileNotFoundException e) {
							System.out.println("An error occurred.");
							e.printStackTrace();
						}

//						DBMessage message = new DBMessage(RequestType.INSERT, "1", " \"Roopana \" , 35 ");
//						dos.writeUTF(objMapper.writeValueAsString(message));
//						System.out.println("Message sent from source to publisher:"+ objMapper.writeValueAsString(message));
//
//        		        DBMessage message = new DBMessage(RequestType.READ, "134", "Sample record");
//						dos.writeUTF(objMapper.writeValueAsString(message));
//						System.out.println("Message sent from source to publisher:"+ objMapper.writeValueAsString(message));
						
					} catch (Exception e) {
						System.out.println("Exception in message source thread "+e.getMessage());
						//TODO close socket connection
					}
				//}
			}

			private Set<String> getTableNames(String queryString) {
				Set<String> tableNames = new HashSet<String>();
				
			
				try {
					int firstIndex = queryString.indexOf(":");
				    int lastIndex = queryString.lastIndexOf(":");
				    String sub = queryString.substring(firstIndex+1, lastIndex);
				    System.out.println("new substring "+sub );
				    String[] arr = sub.split(",");
				    for(int i = 0; i < arr.length; ++i){
				    tableNames.add(arr[i]);
				    }
				    System.out.println("new tableNames "+tableNames );
				} catch (Exception e) {
					System.out.println("Table name not found "+e.getMessage());
				}
				
				return tableNames;
			}
		};
		send.start();
	}
}
