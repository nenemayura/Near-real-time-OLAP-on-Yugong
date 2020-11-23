package com.communication;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Queue;

import com.fasterxml.jackson.databind.ObjectMapper;


public class Publisher {

    static String pubSvrIp = "localhost";
    static String messageSourceIp = "localhost";
    static int pubPort = 5432;
    static int messageSourcePort = 6432;
    
    static ServerSocket pubSocket;
    static ServerSocket pubSourceSocket;
    
    static Queue<DBMessage> inputMessages = new LinkedList<DBMessage>();
    
    //method to publish notification whenever DB entry is created
	public static void main(String args[]) {
		if(args.length >0) {
			if(args[0] != null) {
				pubSvrIp = args[0];
			}
			if(args[1] != null) {
				pubPort = Integer.valueOf(args[1]);
			}
			if(args[2]!= null) {
				messageSourceIp = args[2];
			}
			if(args[3]!= null) {
				messageSourcePort = Integer.valueOf(args[3]);
			}
		}
		listenSource();
		publishToClient();		
	}
	public Publisher(int senderPort, int messageSourceConnPort) {
		this.pubPort = senderPort;
		this.messageSourcePort = messageSourceConnPort;
	}
	public static void publishToClient(){
		try {
			pubSocket = new ServerSocket(pubPort);
			pubSourceSocket = new ServerSocket(messageSourcePort);
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
        				ObjectMapper objMapper = new ObjectMapper();
        				DataInputStream dis = new DataInputStream(nodeSocket.getInputStream());
        				System.out.println("Input msg size is "+inputMessages.size());
//        				if(dis.available() > 0) {
//							final String result  = dis.readUTF();
//							System.out.println("Result received from subscriber "+result);
//						}

        				while (inputMessages.size() < 1) {
        					Thread.sleep(500);
        				}
        				DBMessage messageToPublish = inputMessages.poll();
						
						DataOutputStream dos = new DataOutputStream(nodeSocket.getOutputStream());

						dos.writeUTF(objMapper.writeValueAsString(messageToPublish));
						System.out.println("Mesage from publisher to subscriber:"+ objMapper.writeValueAsString(messageToPublish));
						
						// TODO: check how much delay should be added. It is required to read data from socket else it keeps waiting in input msg queue 
						// while loop
						
						//Thread.sleep(5000);
						System.out.println("Thread woke up after 5 sec delay");
						
						
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
    // collect messages at publisher to a queue and keep sending them to subscriber
    
    public static void listenSource() {

    	Thread listen = new Thread() {
			public void run() {
		    	Socket pubSourceSocket = null;
		    	try {
		    		pubSourceSocket = new Socket(messageSourceIp, messageSourcePort);
		    		System.out.println("Started listeining to message source:" + messageSourcePort);
		    		
		    	} catch (IOException e1) {
		    		e1.printStackTrace();
		    	}
		    	//TODO check if this while loop is required
		     while(true) { // loop because publisher needs to keep listening to the source all the time
		    		DataInputStream dis;
					try {
						dis = new DataInputStream(pubSourceSocket.getInputStream());

						while (dis.available() < 1) {
							Thread.sleep(500);
						}
						ObjectMapper objMapper = new ObjectMapper();
						String received = dis.readUTF();
						System.out.println("received from source:"+ received);
						DBMessage inputMessage =  objMapper.readValue(received, DBMessage.class);
						
						//if(filterMessages(inputMessage) != null) {
							inputMessages.add(inputMessage);
						//}
						
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
		    	}
			}
			};
			listen.start();
    }
}
