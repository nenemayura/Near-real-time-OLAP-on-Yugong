
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
					e1.printStackTrace();
				}
				System.out.println("Starting thread at message source to send messages to publisher");
				//while (true) {
					try {
//        				ObjectMapper objMapper = new ObjectMapper();
//						DataOutputStream dos = new DataOutputStream(publisherSocket.getOutputStream());

        				
//        				DBMessage message = new DBMessage(RequestType.READ, "1", "");
//						dos.writeUTF(objMapper.writeValueAsString(message));
//						System.out.println("Message sent from source to publisher:"+ objMapper.writeValueAsString(message));
						//Read 10 times and write/update once

						try{
							Scanner readQueries;
							File readQueriesFile;
							Scanner updateQueries;
							Scanner insertQueries;

							try{
								readQueriesFile = new File("tpch-stream.sql");
								readQueries = new Scanner(readQueriesFile).useDelimiter(";");
							}
							catch (FileNotFoundException e) {
								System.out.println("An error occurred.");
								e.printStackTrace();
								throw new FileNotFoundException();

							}
							try{
								updateQueries = new Scanner(new File("update.tbl"));
							}
							catch (FileNotFoundException e) {
								System.out.println("An error occurred.");
								e.printStackTrace();
								throw new FileNotFoundException();
							}
							try{
								insertQueries = new Scanner(new File("insert.tbl"));
							}
							catch (FileNotFoundException e) {
								System.out.println("An error occurred.");
								e.printStackTrace();

								throw new FileNotFoundException();
							}

							int readOffset = 0;


							while(true){

								while (readQueries.hasNextLine() && readOffset<10) {
									String readQuery = readQueries.nextLine();
									System.out.println(readQuery);
									readOffset++;
								}
								if (readOffset==10)
									readOffset=0;
								if (!readQueries.hasNextLine())
									readQueries = new Scanner(readQueriesFile).useDelimiter(";");
								if (updateQueries.hasNextLine()){
									String updateQuery = updateQueries.nextLine();
									System.out.println(updateQuery);
								}
								else
									break;
								if (insertQueries.hasNextLine()){
									String insertQuery = insertQueries.nextLine();
									System.out.println(insertQuery);
								}
								else
									break;

							}

							readQueries.close();
							updateQueries.close();
							insertQueries.close();
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
