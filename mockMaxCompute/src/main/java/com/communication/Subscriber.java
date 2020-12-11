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
				DataOutputStream dosSubToPub;
				try {
					subToPubSocket = new Socket(pubSvrIp, port_listen_to);
					disSubFromPub = new DataInputStream(subToPubSocket.getInputStream());
					dosSubToPub = new DataOutputStream(subToPubSocket.getOutputStream());
					ObjectMapper objMapper = new ObjectMapper();
					
					System.out.println("sub socket created");
					while(true) {
							
						while (disSubFromPub.available() < 1) {
							Thread.sleep(100);
						}
						String received = disSubFromPub.readUTF();
						
						DBMessage messageReceived = objMapper.readValue(received, DBMessage.class);
						//TODO read from table in DB
						DBMessage response = messageReceived;
						if(response!= null) {
							response.setReqType(RequestType.ACK_INSERT);
							response.setSenderId(subToPubSocket.getLocalAddress()+"_"+ subToPubSocket.getLocalPort());
							dosSubToPub.writeUTF(objMapper.writeValueAsString(response));
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
// java -jar dbMessageSource.jar 10.32.102.42 6432
// java -jar publisher.jar 10.32.102.43 5432 10.32.102.42 6432
// java -jar subscriber.jar '10.32.102.43' 5432
//java -jar subscriber.jar '10.32.102.43' 5433 - does same ip work?
