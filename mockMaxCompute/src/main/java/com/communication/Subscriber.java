package com.communication;
import com.dbOperations.DBOperationManager;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class Subscriber {
	
	public static Socket subToPubSocket;
    static String pubSvrIp = "localhost";
    static int port_listen_to = 5432;
    public DBOperationManager dbOperationManager;
    public Subscriber() {
    	dbOperationManager = new DBOperationManager();
    }
	
	public static void main(String args[]) {
		if(args.length >0 ) {
			if(args[0]!= null) {
				pubSvrIp = args[0];
			}
			if(args[1] !=null) {
				port_listen_to = Integer.valueOf(args[1]);
			}
		}
		Subscriber subscriber = new Subscriber();
		subscriber.subscribe(port_listen_to);
	}

	public void subscribe(final int port_listen_to) {
		Thread listen = new Thread() {
			public void run() {
				System.out.println("subscribing to port:" + port_listen_to);
				DataInputStream disSubFromPub;
				DataOutputStream dosSubFromPub;
				try {
					subToPubSocket = new Socket(pubSvrIp, port_listen_to);
					disSubFromPub = new DataInputStream(subToPubSocket.getInputStream());
					dosSubFromPub = new DataOutputStream(subToPubSocket.getOutputStream());
					System.out.println("sub socket created");
					while(true) {
							
						while (disSubFromPub.available() < 1) {
							Thread.sleep(500);
						}
						String received = disSubFromPub.readUTF();
						System.out.println("Message received from node:" + received);

						DBMessage messageReceived = new ObjectMapper().readValue(received, DBMessage.class);
						final String result = dbOperationManager.processMessageRequest(messageReceived);
						Thread.sleep(10);
						if(!result.isEmpty()) {
							System.out.println("Result "+result+" is not empty, writing to socket");
							dosSubFromPub.writeUTF(result);

						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		listen.start();
	}
}
