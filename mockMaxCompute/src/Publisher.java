
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import com.fasterxml.jackson.databind.ObjectMapper;


public class Publisher {
    static int pubPort = 5432;
    static ServerSocket pubSocket;
    static String pubSvrIp = "localhost";
    
    //method to publish notification whenever DB entry is created
	public static void main(String args[]) {
		pubPort = Integer.valueOf(args[0]);
		publishToClient();
		
	}
	public Publisher(int senderPort) {
		this.pubPort = senderPort;
	}
	public static void publishToClient(){
		try {
			pubSocket = new ServerSocket(pubPort);
			System.out.println("Started publisher on port:" + pubPort);
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		Thread acceptSubscriptions = new Thread() {
			public void run() {
				while (true) {
					Socket nodeSocket;
					try {
						nodeSocket = pubSocket.accept();
						System.out.println("Accepted connection from subscriber");
                        publish(nodeSocket);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		};
		acceptSubscriptions.start();
	}
    public static void publish(Socket nodeSocket) {
		
		Thread publish = new Thread() {
			public void run() {
				System.out.println("Starting thread to publish messages to every node");
				while (true) {
					try {
						DataOutputStream dos = new DataOutputStream(nodeSocket.getOutputStream());
						DBMessage messageToPublish = new DBMessage(RequestType.READ, "123", "Sample record");
						ObjectMapper objMapper = new ObjectMapper();

						dos.writeUTF(objMapper.writeValueAsString(messageToPublish));
						System.out.println("Mesage from publisher:"+ objMapper.writeValueAsString(messageToPublish));

						DataInputStream dis = new DataInputStream(nodeSocket.getInputStream());
						while (dis.available() < 1) {
							Thread.sleep(500);
						}
						String received = dis.readUTF();
						DBMessage messageReceived = objMapper.readValue(received, DBMessage.class);

						System.out.println("Message received from subscriber:" + messageReceived.toString());
						Thread.sleep(7000);
						
					} catch (Exception e) {
						System.out.println("Exception in publish thread");
					}
				}
			}
		};
		publish.start();
	}
    
    public static DBMessage filterMessages(DBMessage dbMessage) {
		if(dbMessage.getReqType() == RequestType.READ) {
			return dbMessage;
		}
    	return null;
    	
    }
}
