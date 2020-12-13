import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

//import DatabaseConnection;
//import StateTableOperationManager;

public class Publisher {

	static String pubSvrIp = "localhost";
	static String messageSourceIp = "localhost";
	static int pubPort = 5432;
	static int messageSourcePort = 6432;

	static ServerSocket pubSocket;
	static Socket pubSourceSocket;

	static volatile Queue<DBMessage> inputMessages = new LinkedList<DBMessage>();
	// No of acks received from each key
	public static volatile ConcurrentHashMap<String, Set<String>> ackMap = new ConcurrentHashMap<String, Set<String>>();
	public static volatile ConcurrentHashMap<String, Set<String>> stateTable = new ConcurrentHashMap<String, Set<String>>();
	public static ConcurrentHashMap<String, Socket> subscriberNodeSocketMap = new ConcurrentHashMap<String, Socket>();
	public static int Nw = 2; // TODO remove hardcoding
	public static volatile List<RequestStat> requestStats = new ArrayList<RequestStat>();
	public static ConcurrentHashMap<String, Set<String>> subscriberReplicaMap = new ConcurrentHashMap<String, Set<String>>();


	// method to publish notification whenever DB entry is created
	public static void main(String args[]) {
		if (args.length > 0) {
			if (args[0] != null) {
				pubSvrIp = args[0];
			}
			if (args[1] != null) {
				pubPort = Integer.valueOf(args[1]);
			}
			if (args[2] != null) {
				messageSourceIp = args[2];
			}
			if (args[3] != null) {
				messageSourcePort = Integer.valueOf(args[3]);
			}
		}
		listenSource();
		acceptSubscriptions();
		publish(); // thread to broadcast msgs to subscribers
		writeStatsToFile(20000); // write to json every 1 min

	}

	public Publisher(int senderPort, int messageSourceConnPort) {
		this.pubPort = senderPort;
		this.messageSourcePort = messageSourceConnPort;
	}

	public static void acceptSubscriptions() {
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
						String nodeKey = nodeSocket.getRemoteSocketAddress().toString();
						System.out.println("nodeKey:" + nodeKey);

						subscriberNodeSocketMap.put(nodeKey, nodeSocket);
						System.out.println("Accepted connection from subscriber");

						listenAck(nodeSocket);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		};
		acceptSubscriptions.start();

	}

	public static void writeStatsToFile(int t) { // writeStatsToFile every t milliseconds
		Thread writeLog = new Thread() {
			public void run() {
				BufferedWriter writer = null;
				while (true) {
					System.out.println("size of stats when checked:"+ requestStats.size());
					if (requestStats.size() > 10) {
						List<RequestStat> temp = new ArrayList<RequestStat>();
						temp.addAll(requestStats);
						
						ObjectMapper objectMapper = new ObjectMapper();
						try {
							String statStr = objectMapper.writeValueAsString(requestStats);
							requestStats = new ArrayList<RequestStat>();
							writer = new BufferedWriter(new FileWriter("log_data.json", true));
							writer.write(statStr+"\n");
							writer.close();
							System.out.println("wrote to file:"+ statStr);

						} catch (IOException e) {
							e.printStackTrace();
						} finally {
							try {
								if (writer != null) {
									writer.close();
								}
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					} // end if
					try {
						Thread.sleep(t);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} // end while
			}

		};
		writeLog.start();
	}

	public static void publish() { // publishes or broadcasts to all subscribers
		Thread publish = new Thread() {
			public void run() {
				System.out.println(
						"Starting thread to publish messages to every node input messg size " + inputMessages.size());
				while (true) {
					try {
						ObjectMapper objMapper = new ObjectMapper();

						while (inputMessages.size() < 1) {
							Thread.sleep(500);
						}
						DBMessage messageToPublish = inputMessages.poll();

						Iterator<Map.Entry<String, Socket>> iterator = subscriberNodeSocketMap.entrySet().iterator();
						System.out.println("Writing msg to subscribers");
						while (iterator.hasNext()) {
							Entry<String, Socket> entry = iterator.next();
							Socket nodeSocket = entry.getValue();

							try {
								DataOutputStream dos = new DataOutputStream(nodeSocket.getOutputStream());
								dos.writeUTF(objMapper.writeValueAsString(messageToPublish));
								System.out.println(" msg to subscribers .... " + messageToPublish);

							} catch (Exception e) {
								System.out.println("Exception while broadcasting, closing connection");
								e.printStackTrace();
								nodeSocket.close();
								iterator.remove();
							}
						}
					} catch (Exception e) {
						System.out.println("Exception in publish thread");
						e.printStackTrace();

					}
				}
			}
		};
		publish.start();
	}


	public static void listenSource() {

		Thread listen = new Thread() {
			public void run() {
				try {
					pubSourceSocket = new Socket(messageSourceIp, messageSourcePort);
					System.out.println("Started listeining to message source:" + messageSourcePort);

				} catch (IOException e1) {
					e1.printStackTrace();
				}
				while (true) { // loop because publisher needs to keep listening to the source all the time
					DataInputStream dis;
					try {
						dis = new DataInputStream(pubSourceSocket.getInputStream());

						while (dis.available() < 1) {
							Thread.sleep(500);
						}
						ObjectMapper objMapper = new ObjectMapper();
						String received = dis.readUTF();

						DBMessage inputMessage = objMapper.readValue(received, DBMessage.class);

						Date date = new Date();
						long startTime = date.getTime();
						inputMessage.setStartTime(startTime);

						if (inputMessage.getReqType().equals(RequestType.TPC_READ)) {
							System.out.println("Added tpc read");

							inputMessages.add(inputMessage);
						}

						if (inputMessage.getReqType() == RequestType.INSERT
								|| inputMessage.getReqType() == RequestType.DELETE
								|| inputMessage.getReqType() == RequestType.EDIT) {
							inputMessages.add(inputMessage); // this queue broadcasts msgs to all subscribers in the
																// network

						} else if (inputMessage.getReqType() == RequestType.READ) {

							Socket readTargetNodeSocket = getNodeWithUpdatedState(inputMessage, inputMessage.getTableNames());
							if ( readTargetNodeSocket!= null) {
								DataOutputStream dos = new DataOutputStream(readTargetNodeSocket.getOutputStream());
								dos.writeUTF(objMapper.writeValueAsString(inputMessage));
								System.out.println("wrote messsage to subscriber");
							}

						}

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

	public static void listenAck(final Socket nodeSocket) {

		Thread listenAck = new Thread() {
			public void run() {

				while (true) { // loop because publisher needs to keep listening to the source all the time
					DataInputStream dis;
					try {
						dis = new DataInputStream(nodeSocket.getInputStream());

						while (dis.available() < 1) {
							Thread.sleep(500);
//							long endTime = new Date().getTime();
//							RequestStat reqStat = new RequestStat(endTime-10, endTime,
//									"READ", true, 1, Nw, 3);
//							requestStats.add(reqStat);
//							System.out.println("Size of stats after adding:"+ requestStats.size());
						}
						ObjectMapper objMapper = new ObjectMapper();
						String received = dis.readUTF();

						DBMessage inputMessage = objMapper.readValue(received, DBMessage.class);
						int numNodes = subscriberNodeSocketMap.size();

						if (inputMessage.getReqType() == RequestType.ACK_INSERT
								|| inputMessage.getReqType() == RequestType.ACK_DELETE
								|| inputMessage.getReqType() == RequestType.ACK_EDIT) {

							System.out.println("listening to ack");
							String requestKey = inputMessage.getMessageKey();

							synchronized (this) {

								updateAckMap(requestKey, inputMessage.getSenderId());
								Set<String> senderNodes = ackMap.get(requestKey);

								int nAcks = senderNodes != null ? senderNodes.size() : 0;

								if (nAcks >= Nw) {
									System.out.println("nAcks:" + nAcks + " nAcksReqd:" + Nw);

									/**
									 * once the request is successful flush the ack entry from map to enable
									 * processing request on same entry by same client
									 **/
									long endTime = new Date().getTime();
									RequestStat reqStat = new RequestStat(inputMessage.getStartTime(), endTime,
											inputMessage.getReqType().name(), true, 1, Nw, numNodes);
									requestStats.add(reqStat);
									populateStateTable(inputMessage.getTableName(), ackMap.get(requestKey));
									updateDataBase(inputMessage.getTableName(), ackMap.get(requestKey));
									flushKeyFromAckMap(requestKey);

									// TODO send response to user
								}
							}
						} else if (inputMessage.getReqType().equals(RequestType.READ_RESPONSE)) {
							System.out.println("Read response received " + inputMessage.getRecord());
							long endTime = new Date().getTime();
							RequestStat reqStat = new RequestStat(inputMessage.getStartTime(), endTime,
									inputMessage.getReqType().name(), true, 1, Nw, numNodes);
							requestStats.add(reqStat);
						}
						
					} catch (IOException e) {
						e.printStackTrace();
						System.out.println("Exception occured, closing connection");
						try {
							nodeSocket.close();
							Thread.currentThread().interrupt();
						} catch (IOException e1) {
							e1.printStackTrace();
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
						System.out.println("Exception occured, closing connection");
						try {
							nodeSocket.close();
							Thread.currentThread().interrupt();
						} catch (IOException e1) {
							e1.printStackTrace();
						}
					}
				}
			}
		};
		listenAck.start();
	}

	public static void populateStateTable(String tableName, Set<String> set) {
		int maxStateTableSize = 1000;

		final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
		rwl.readLock().lock();
		List<String> recordIds = new ArrayList<String>(stateTable.keySet());
		rwl.readLock().unlock();

		rwl.writeLock().lock();
		if (recordIds.size() > maxStateTableSize) {
			stateTable.remove(recordIds.get(0));
		}
		stateTable.put(tableName, set);
		updateDataBase(tableName, set);

		rwl.writeLock().unlock();
		System.out.println("Updated state table with name:" + tableName + "entries:" + set);
	}

	public static void updateDataBase(String tableName, Set<String> set) {
		Connection localConnection = DatabaseConnection.getConnection();
		Statement stmt;

		try {
			stmt = localConnection.createStatement();
			String nodes = "";
			for (String nodeId : set) {
				nodes = nodes + "," + nodeId;
			}
			stmt.executeUpdate("INSERT INTO statetable(id, tablename, nodes) VALUES('" + 1 + "','" + tableName + "','"
					+ nodes + "')" + "ON DUPLICATE KEY UPDATE nodes= '" + nodes + "'");

		} catch (SQLException e) {
			System.out.println("Error while executing statement " + e);
		}
	}	
	
	public static Socket getNodeWithUpdatedState(DBMessage inputMessage, Set<String> tableNames) {
		List<Set<String>> nodeIdsTableWise = new ArrayList<Set<String>>();
		for(String tableName: tableNames) {
			System.out.println("gettting node for tableName " + tableName);
			if (stateTable.get(tableName) != null) { // if the record is present in state table return any node in the list
				Set<String> temp = stateTable.get(tableName);
				nodeIdsTableWise.add(temp);
			}else {
				final Connection con = DatabaseConnection.getConnection();
				StateTableOperationManager stateTableHandler = new StateTableOperationManager(con);
				String nodeIdConcatnated  = stateTableHandler.readFromStateTable("0", tableName); // record id, table name
				
				String[] nodesIdSplit = nodeIdConcatnated.split(DatabaseConstants.COMMA_DELIMETER);
				String[] nodesIdsClean = cleanNodeId(nodesIdSplit);
				Set<String> table_nodes = new HashSet<String>(Arrays.asList(nodesIdsClean));
				nodeIdsTableWise.add(table_nodes);
				System.out.println("Node id received from the state table " + nodesIdSplit);
			}
		}
		Set<String> nodeIdsStateTable = getMaxIntersection(nodeIdsTableWise);
		String maxIntNode = "";
		Set<String> maxIntTables = Collections.EMPTY_SET;
		if(nodeIdsStateTable.isEmpty()) { // if state table is empty check for rep logic
			String maxIntNodeid = "";
			int maxIntSize = 0;

			for (Entry<String, Set<String>> entry: subscriberReplicaMap.entrySet()){
				
				Set<String> intersection = new HashSet<String>(tableNames);
				intersection.retainAll(entry.getValue());
				
				if(intersection.size()> maxIntSize) {
					maxIntSize = intersection.size();
					maxIntNode = entry.getKey();
					maxIntTables = entry.getValue();
				}
			}
		}else if(nodeIdsStateTable.size()>1) {
			// TODO check for bandwidth between nodes
			maxIntNode = nodeIdsStateTable.iterator().next();
		}
		tableNames.removeAll(maxIntTables);
		requestRepTables(inputMessage, subscriberNodeSocketMap.get(maxIntNode), tableNames);
		
		if (subscriberNodeSocketMap.containsKey(maxIntNode)) {
			System.out.println("Subscriber node map contains the " + maxIntNode);
			Socket readNodeSocket = subscriberNodeSocketMap.get(maxIntNode);
			return readNodeSocket;
		}
		return null;
	}
	
	public static void requestRepTables(DBMessage inputMessage, Socket subSocket, Set<String> repTables){

		ObjectMapper objMapper = new ObjectMapper();
		DataOutputStream dos;
		try {
			dos = new DataOutputStream(subSocket.getOutputStream());
			if (repTables.size()>0) {
				inputMessage.setReqType(RequestType.REP_TABLES);
				inputMessage.setTableNames(repTables);
				dos.writeUTF(objMapper.writeValueAsString(inputMessage));

			}else { // rep tables size <=0 no rep required
				inputMessage.setReqType(RequestType.TPC_READ);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("wrote Rep Request to subscriber");
	}
	public static Set<String> getMaxIntersection(List<Set<String>> nodeIdsTableWise) {
		if(nodeIdsTableWise.size() ==0) {
			return Collections.EMPTY_SET;
		}
		
		Set<String> result = nodeIdsTableWise.get(0);
		for(Set<String> nodeIdsPerTable: nodeIdsTableWise) {
			result.retainAll(nodeIdsPerTable);			
		}
		return result;
				
	}

	public static String[] cleanNodeId(String[] nodesIdSplit ) {
		String[] nodesIdsClean = new String[nodesIdSplit.length];
		for(int i=0; i<nodesIdSplit.length; i++ ) {
			String nodeId = nodesIdSplit[i];
			StringBuffer sb = new StringBuffer(nodeId);
			sb.deleteCharAt(0);
			nodeId = sb.toString();
			nodeId = nodeId.replace("_", ":");
			nodesIdsClean[i] =nodeId;
		}
		return nodesIdsClean;
	}

	public synchronized static void updateAckMap(String ackMapkey, String nodeId) {
		final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
		rwl.readLock().lock();
		if (ackMap.get(ackMapkey) != null) {
			rwl.readLock().unlock();
			rwl.writeLock().lock();
			Set<String> ackNodes = ackMap.get(ackMapkey);
			ackNodes.add(nodeId);
			ackMap.put(ackMapkey, ackNodes);
			rwl.writeLock().unlock();
		} else {
			rwl.readLock().unlock();
			rwl.writeLock().lock();
			Set<String> ackNodes = new HashSet<String>();
			ackNodes.add(nodeId);
			ackMap.put(ackMapkey, ackNodes);
			rwl.writeLock().unlock();
		}
	}

	public synchronized static void flushKeyFromAckMap(String ackMapkey) {
		final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
		rwl.readLock().lock();
		if (ackMap.get(ackMapkey) != null) {
			rwl.readLock().unlock();
			rwl.writeLock().lock();
			ackMap.remove(ackMapkey);
			rwl.writeLock().unlock();
		} else {
			rwl.readLock().unlock();
		}
		System.out.println("flushed key from ackmap," + ackMapkey);
	}

}

//public static Socket getNodeFromRepLogic(List<String> tableNames, String[] nodesIds) {
//List<String> nodeIds = new ArrayList<String>();
//
//System.out.println("gettting node for record id " + recordId);
//String nodeId = "";
//if (stateTable.get(recordId) != null) { // if the record is present in state table return any node in the list
//	nodeId = stateTable.get(recordId).iterator().next();
//} else { // if record has not been updated at all
//			// TODO implement logic to look up in local DB and other DCs
////	Iterator<String, Set<TableNames>> it = subscriberReplicaMap.iterator();
//	Set<TableNames> max = null;
//	while(it.hasNext()){
//		Entry<String, Set<TableNames>>entry = it.next();
//		Set<TableNames> intersection = reqTables.retainAll(entry.getValue());
//		if (max==null) {
//			max = intersection;
//			nodeId = entry.getKey();
//		}
//		else if (max.size() < intersection.size()) {
//			max = entry.getValue();
//			nodeId = entry.getKey();
//		}
//	}
//	reqTables.removeAll(max);
//	final Connection con = DatabaseConnection.getConnection();
//	StateTableOperationManager stateTableHandler = new StateTableOperationManager(con);
//	nodeId = stateTableHandler.readFromStateTable(recordId, "testtable");
//	nodeId = "testNodeId";
//	System.out.println("Node id received from the state table " + nodeId);
//	StringBuffer sb = new StringBuffer(nodeId);
//	sb.deleteCharAt(0);
//	nodeId = sb.toString();
//	nodeId = nodeId.replace("_", ":");
//	System.out.println("new Node id received from the state table " + nodeId);
//}
//if (subscriberNodeSocketMap.containsKey(nodeId)) {
//	System.out.println("Subscriber node map contains the " + nodeId);
//	Socket readNodeSocket = subscriberNodeSocketMap.get(nodeId);
//	return readNodeSocket;
//}
//
//return null;
//
//return null;
//
//}
//

//public static Socket getNodeWithUpdatedState(List<String> tableName) {
//System.out.println("gettting node for tableName " + tableName);
//String nodeId = "";
//if (stateTable.get(tableName) != null) { // if the record is present in state table return any node in the list
//	nodeId = stateTable.get(tableName).iterator().next();
//} else { // if record has not been updated at all
//			// TODO implement logic to look up in local DB and other DCs
//	final Connection con = DatabaseConnection.getConnection();
//	StateTableOperationManager stateTableHandler = new StateTableOperationManager(con);
//	nodeId = stateTableHandler.readFromStateTable(tableName, "testtable");
//	nodeId = "testNodeId";
//	System.out.println("Node id received from the state table " + nodeId);
//	StringBuffer sb = new StringBuffer(nodeId);
//	sb.deleteCharAt(0);
//	nodeId = sb.toString();
//	nodeId = nodeId.replace("_", ":");
//	System.out.println("new Node id received from the state table " + nodeId);
//}
//if (subscriberNodeSocketMap.containsKey(nodeId)) {
//	System.out.println("Subscriber node map contains the " + nodeId);
//	Socket readNodeSocket = subscriberNodeSocketMap.get(nodeId);
//	return readNodeSocket;
//}
//return null;
//}

//public static Socket getNodeWithUpdatedState(String recordId, Set<TableNames> reqTables) {
//System.out.println("gettting node for record id " + recordId);
//String nodeId = "";
//if (stateTable.get(recordId) != null) { // if the record is present in state table return any node in the list
//	nodeId = stateTable.get(recordId).iterator().next();
//} else { // if record has not been updated at all
//			// TODO implement logic to look up in local DB and other DCs
//	Iterator<String, Set<TableNames>> it = subscriberReplicaMap.iterator();
//	Set<TableNames> max = null;
//	while(it.hasNext()){
//		Entry<String, Set<TableNames>>entry = it.next();
//		Set<TableNames> intersection = reqTables.retainAll(entry.getValue());
//		if (max==null) {
//			max = intersection;
//			nodeId = entry.getKey();
//		}
//		else if (max.size() < intersection.size()) {
//			max = entry.getValue();
//			nodeId = entry.getKey();
//		}
//	}
//	
//	final Connection con = DatabaseConnection.getConnection();
//	StateTableOperationManager stateTableHandler = new StateTableOperationManager(con);
//	nodeId = stateTableHandler.readFromStateTable(recordId, "testtable");
//	nodeId = "testNodeId";
//	System.out.println("Node id received from the state table " + nodeId);
//	StringBuffer sb = new StringBuffer(nodeId);
//	sb.deleteCharAt(0);
//	nodeId = sb.toString();
//	nodeId = nodeId.replace("_", ":");
//	System.out.println("new Node id received from the state table " + nodeId);
//}
//if (subscriberNodeSocketMap.containsKey(nodeId)) {
//	System.out.println("Subscriber node map contains the " + nodeId);
//	Socket readNodeSocket = subscriberNodeSocketMap.get(nodeId);
//	return readNodeSocket;
//}
//
//return null;
//
//}
