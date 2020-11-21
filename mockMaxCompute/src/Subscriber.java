import java.io.DataInputStream;
import java.net.Socket;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Subscriber {
	
	public static Socket subToPubSocket;
    static String pubSvrIp = "localhost";
    static int port_listen_to = 5432;
	
	public static void main(String args[]) {
		if(args.length >0 ) {
			if(args[0]!= null) {
				pubSvrIp = args[0];
			}
			if(args[1] !=null) {
				port_listen_to = Integer.valueOf(args[1]);
			}
		}
		subscribe(port_listen_to);
	}

	public static void subscribe(int port_listen_to) {
		Thread listen = new Thread() {
			public void run() {
				System.out.println("subscribing to port:" + port_listen_to);
				DataInputStream disSubFromPub;
				try {
					subToPubSocket = new Socket(pubSvrIp, port_listen_to);
					disSubFromPub = new DataInputStream(subToPubSocket.getInputStream());
	
					System.out.println("sub socket created");
					while(true) {
							
						while (disSubFromPub.available() < 1) {
							Thread.sleep(500);
						}
						String received = disSubFromPub.readUTF();
						DBMessage messageReceived = new ObjectMapper().readValue(received, DBMessage.class);
	
						System.out.println("Message received from node:" + received);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		listen.start();
	}
}
