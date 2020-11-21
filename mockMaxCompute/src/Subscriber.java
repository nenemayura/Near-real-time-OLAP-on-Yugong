import java.io.DataInputStream;
import java.net.Socket;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Subscriber {
	
	public static Socket subToPubSocket;
    static String pubSvrIp = "localhost";

	
	public static void main(String args[]) {
		int port_listen_to = Integer.valueOf(args[0]);
		System.out.println("Arg received:" + port_listen_to);
		subscribe(port_listen_to);
	}

	public static void subscribe(int port_listen_to) {
		Thread listen = new Thread() {
			public void run() {
				System.out.println("subscribing to port:" + port_listen_to);
				DataInputStream disSubFromPub;
				
				try {
					subToPubSocket = new Socket(pubSvrIp, port_listen_to);
					System.out.println("sub socket created");
					disSubFromPub = new DataInputStream(subToPubSocket.getInputStream());
					while (disSubFromPub.available() < 1) {

					}
					String received = disSubFromPub.readUTF();
					DBMessage messageReceived = new ObjectMapper().readValue(received, DBMessage.class);

					System.out.println("Message received from node:" + messageReceived);
					Thread.sleep(7000);

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		listen.start();
	}
}
