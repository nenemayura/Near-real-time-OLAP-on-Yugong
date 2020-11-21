
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;


import com.fasterxml.jackson.databind.ObjectMapper;


public class DBMessageSource {
	public static ServerSocket sourceToPubSocket;
    static String messageSourceIp = "localhost";
    static int messageSourcePort = 6432;
   
    // give args as ip, port 
   
	public static void main(String args[]) {
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
				while (true) {
					try {
        				ObjectMapper objMapper = new ObjectMapper();
						DataOutputStream dos = new DataOutputStream(publisherSocket.getOutputStream());

        				
        				DBMessage message = new DBMessage(RequestType.READ, "123", "Sample record");
						dos.writeUTF(objMapper.writeValueAsString(message));
						System.out.println("Message sent from source to publisher:"+ objMapper.writeValueAsString(message));
						
        		        message = new DBMessage(RequestType.EDIT, "123", "Sample record");
						dos.writeUTF(objMapper.writeValueAsString(message));
						System.out.println("Message sent from source to publisher:"+ objMapper.writeValueAsString(message));
						
        		        message = new DBMessage(RequestType.READ, "134", "Sample record");
						dos.writeUTF(objMapper.writeValueAsString(message));
						System.out.println("Message sent from source to publisher:"+ objMapper.writeValueAsString(message));
						
					} catch (Exception e) {
						System.out.println("Exception in message source thread");
						//TODO close socket connection
					}
				}
			}
		};
		send.start();
	}
}
