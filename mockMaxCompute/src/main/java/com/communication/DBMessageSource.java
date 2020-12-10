package com.communication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.xml.internal.fastinfoset.util.StringArray;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;


public class DBMessageSource {
	public static ServerSocket sourceToPubSocket;
    static String messageSourceIp = "localhost";
    static int messageSourcePort = 6432;
   
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
					System.out.println("An error occurred.");
					e1.printStackTrace();
				}
				System.out.println("Starting thread at message source to send messages to publisher");
				//while (true) {
					try {
        				ObjectMapper objMapper = new ObjectMapper();
						DataOutputStream dos = new DataOutputStream(publisherSocket.getOutputStream());

        				
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
								readQueriesFile = new File("tpch-stream.sql");
								readQueryScanner = new Scanner(readQueriesFile).useDelimiter(";");
							}
							catch (FileNotFoundException e) {
								System.out.println("An error occurred.");
								e.printStackTrace();
								throw new FileNotFoundException();

							}
							try{
								updateQueryScanner = new Scanner(new File("update.tbl.u1"));
							}
							catch (FileNotFoundException e) {
								System.out.println("An error occurred.");
								e.printStackTrace();
								throw new FileNotFoundException();
							}
							try{
								insertQueryScanner = new Scanner(new File("insert.tbl"));
							}
							catch (FileNotFoundException e) {
								System.out.println("An error occurred.");
								e.printStackTrace();

								throw new FileNotFoundException();
							}

							int readOffset = 0;


							while(true){

								while (readQueryScanner.hasNext() && readOffset<10) {
									Query readQuery = new Query(RequestType.READ, readQueryScanner.next()+";");
									System.out.println(readQuery.getQuery());
									readOffset++;
								}
								if (readOffset==10)
									readOffset=0;
								if (!readQueryScanner.hasNext())
									readQueryScanner = new Scanner(readQueriesFile).useDelimiter(";");
								if (updateQueryScanner.hasNextLine()){
									Query updateQuery = new Query(RequestType.EDIT,updateQueryScanner.nextLine(),TableName.customer);
									System.out.println(updateQuery.getQuery());
								}
								else
									break;
								if (insertQueryScanner.hasNextLine()){
									Query insertQuery = new Query(RequestType.INSERT,insertQueryScanner.nextLine(),TableName.customer);
									System.out.println(insertQuery.getQuery());
								}
								else
									break;

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
						System.out.println("Exception in message source thread");
						//TODO close socket connection
					}
				//}
			}
		};
		send.start();
	}
}
