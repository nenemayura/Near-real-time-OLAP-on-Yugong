package com.communication;
import com.dbOperations.DBOperationManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.replication.ReplicationManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;



public class Subscriber {
	public static replicatedTables;
	public static ReplicationManager replicationManager= new ReplicationManager();;
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
		deleteTables();
	}


	public static void deleteTables() {
		Thread deleteTablesThread = new Thread() {
			public void run() {
				while(true) {
					try {

						this.replicationManager.deleteReplicatedTables();
						
							Thread.sleep(30000);
						} catch (Exception e) {
							System.out.println("Exception occured while deleting tables "+e.getMessage());
						}
					}
				}
		
		};
		deleteTablesThread.start();
		
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
							Thread.sleep(1000);
						}
						String received = disSubFromPub.readUTF();
						System.out.println("Received "+received+" at subscriber ");
						
						DBMessage messageReceived = objMapper.readValue(received, DBMessage.class);
						DBOperationManager dbOperationManager = new DBOperationManager();
						String result = dbOperationManager.processMessageRequest(messageReceived);
						System.out.println("msg received  "+messageReceived);
						DBMessage response = new DBMessage();
						if(messageReceived.getReqType().equals(RequestType.TPC_READ)) {
							result = dbOperationManager.processTpcRead(messageReceived.getRecord());
							System.out.println("The result receivd is "+result);
							response.setReqType(RequestType.READ_RESPONSE);
							response.setRecord(result);
							response.setSenderId(subToPubSocket.getLocalAddress()+"_"+ subToPubSocket.getLocalPort());
							dosSubToPub.writeUTF(objMapper.writeValueAsString(response));
							System.out.println("Wrote response  to publisher  "+result);
						} else if(messageReceived.getReqType().equals(RequestType.INSERT) ) {
							response.setReqType(RequestType.ACK_INSERT);
							response.setRecordId(messageReceived.getRecordId());
							response.setSenderId(subToPubSocket.getLocalAddress()+"_"+ subToPubSocket.getLocalPort());
							dosSubToPub.writeUTF(objMapper.writeValueAsString(response));
							System.out.println("Wrote ack insert to publisher  ");
						} else if (messageReceived.getReqType().equals(RequestType.EDIT)) {
							response.setReqType(RequestType.ACK_EDIT);
							response.setRecordId(messageReceived.getRecordId());
							response.setSenderId(subToPubSocket.getLocalAddress()+"_"+ subToPubSocket.getLocalPort());
							dosSubToPub.writeUTF(objMapper.writeValueAsString(response));
							System.out.println("Wrote ack insert to publisher  ");
						} else if (messageReceived.getReqType().equals(RequestType.READ)) {
							response.setReqType(RequestType.READ_RESPONSE);
							response.setRecord(result);
							response.setSenderId(subToPubSocket.getLocalAddress()+"_"+ subToPubSocket.getLocalPort());
							dosSubToPub.writeUTF(objMapper.writeValueAsString(response));
							System.out.println("Wrote response  to publisher  "+result);
						}
						Thread.sleep(10000);
						//TODO read from table in DB
//						DBMessage response = messageReceived;
//						if(response!= null) {
//							response.setReqType(RequestType.ACK_INSERT);
//							response.setSenderId(subToPubSocket.getLocalAddress()+"_"+ subToPubSocket.getLocalPort());
//							dosSubToPub.writeUTF(objMapper.writeValueAsString(response));
//							System.out.println("Wrote ack insert to publisher  ");
//						}
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
