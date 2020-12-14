package com.communication;
import com.dbOperations.DBOperationManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.replication.ReplicationManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;



public class Subscriber {
	public static List<String>replicatedTables = new ArrayList<String>();
	public static Socket subToPubSocket;
    static String pubSvrIp = "localhost";
    static int port_listen_to = 5432;

    public static Set<String> namesList = new HashSet<String>();
	public static Set<String> allTablesInSub = new HashSet<String>();
	
	public static void main(String args[]) {
		DBOperationManager dbOperationManager = new DBOperationManager();
		//namesList = list of tables already present
		namesList = new HashSet<String>(dbOperationManager.getOwnTables());
		if(args.length >0 ) {
			if(args[0]!= null) {
				pubSvrIp = args[0];
			}
			if(args[1] !=null) {
				port_listen_to = Integer.valueOf(args[1]);
			}
		}
		subscribe(port_listen_to);
	//	deleteTables();
	}


	public static void deleteTables() {
		ReplicationManager replicationManager = new ReplicationManager();

		Thread deleteTablesThread = new Thread() {
			public void run() {
				while(true) {
					try {

						replicationManager.deleteReplicatedTables(replicatedTables);
						
							Thread.sleep(60000);
						} catch (Exception e) {
							System.out.println("Exception occured while deleting tables "+e);
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
						String result = "";
						//String result = dbOperationManager.processMessageRequest(messageReceived);
						System.out.println("msg received  "+messageReceived);
						DBMessage response = new DBMessage();
						response.setStartTime(messageReceived.getStartTime());
						response.setSenderId(subToPubSocket.getLocalAddress()+"_"+ subToPubSocket.getLocalPort());
						
						if(messageReceived.getReqType().equals(RequestType.TPC_READ)) {
							result = dbOperationManager.processTpcRead(messageReceived.getRecord());
							System.out.println("The result receivd is "+result);
							response.setReqType(RequestType.READ_RESPONSE);
							response.setRecord(result);
							allTablesInSub = new HashSet<String>(replicatedTables);
							allTablesInSub.addAll((namesList));
							response.setReplicatedTables(allTablesInSub);
							
							dosSubToPub.writeUTF(objMapper.writeValueAsString(response));
							System.out.println("Wrote response  to publisher  "+result);
						} else if(messageReceived.getReqType().equals(RequestType.INSERT) ) {
							response.setReqType(RequestType.ACK_INSERT);
							response.setRecordId(messageReceived.getRecordId());
							dosSubToPub.writeUTF(objMapper.writeValueAsString(response));
							System.out.println("Wrote ack insert to publisher  ");
						} else if (messageReceived.getReqType().equals(RequestType.EDIT)) {
							if (allTablesInSub.contains("orders")){
							result = dbOperationManager.processUpdate(messageReceived.getRecord());
							response.setReqType(RequestType.ACK_EDIT);
							response.setRecordId(messageReceived.getRecordId());
							dosSubToPub.writeUTF(objMapper.writeValueAsString(response));
							System.out.println("Wrote ack insert to publisher  ");}
							else
								System.out.println("Orders table not present in "+ subToPubSocket.getLocalAddress()+"_"+ subToPubSocket.getLocalPort());
						} else if (messageReceived.getReqType().equals(RequestType.READ)) {
							response.setReqType(RequestType.READ_RESPONSE);
							response.setRecord(result);
							
							response.setSenderId(subToPubSocket.getLocalAddress()+"_"+ subToPubSocket.getLocalPort());
							dosSubToPub.writeUTF(objMapper.writeValueAsString(response));
							System.out.println("Wrote response  to publisher  "+result);
						}
						else if (messageReceived.getReqType().equals(RequestType.REP_TABLES)){
							Map<String,String> tablesToRepli = messageReceived.getTableToNodeMap();
							Iterator<Map.Entry<String, String>> itr = tablesToRepli.entrySet().iterator();
							ReplicationManager replicationManager = new ReplicationManager();
							while(itr.hasNext()){
								Map.Entry<String, String> entry = itr.next();
								Connection rc = replicationManager.getRemoteDatabaseConnection(entry.getValue());
								ResultSet rs = replicationManager.selectDataFromRemote(rc,entry.getKey());
								replicationManager.replicateDataFromRemote(rs,entry.getKey());
								replicatedTables.add(entry.getKey());
							}


							result = dbOperationManager.processTpcRead(messageReceived.getRecord());
							System.out.println("The result receivd is "+result);
							response.setReqType(RequestType.REP_TABLES);
							allTablesInSub = new HashSet<String>(replicatedTables);
							allTablesInSub.addAll((namesList));
							response.setReplicatedTables(allTablesInSub);
							response.setRecord(result);
							response.setSenderId(subToPubSocket.getLocalAddress()+"_"+ subToPubSocket.getLocalPort());
							dosSubToPub.writeUTF(objMapper.writeValueAsString(response));
							System.out.println("Wrote response  to publisher  "+result);
						} else if(messageReceived.getReqType().equals(RequestType.CONSISTENCY_CHECK)) {
							result = dbOperationManager.processConsistencyQuery(messageReceived.getRecord());
							response.setRecord(result);
							response.setReqType(RequestType.ACK_CONSISTENCY_CHECK);
							response.setSenderId(subToPubSocket.getLocalAddress()+"_"+ subToPubSocket.getLocalPort());
							response.setConsistencyNodes(messageReceived.getConsistencyNodes());

							allTablesInSub  = new HashSet<String>(replicatedTables);
							allTablesInSub.addAll((namesList));
							response.setReplicatedTables(allTablesInSub);
							dosSubToPub.writeUTF(objMapper.writeValueAsString(response));
							System.out.println("Wrote response  to publisher  "+result);
						}
						Thread.sleep(10000);
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
