package com.communication;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
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

import com.fasterxml.jackson.databind.ObjectMapper;

public class Publisher {

	static String pubSvrIp = "localhost";
	static String messageSourceIp = "localhost";
	static int pubPort = 5432;
	static int messageSourcePort = 6432;

	static ServerSocket pubSocket;
	static Socket pubSourceSocket;

	static volatile Queue<DBMessage>  inputMessages = new LinkedList<DBMessage>();
	public static volatile ConcurrentHashMap<String, Set<String>> ackMap = new ConcurrentHashMap<String, Set<String>>(); // no
																															// of
																															// acknowledgements
																															// received
																															// for
																															// each
																															// reqKey
	public static volatile ConcurrentHashMap<String, Set<String>> stateTable = new ConcurrentHashMap<String, Set<String>>();
	public static ConcurrentHashMap<String, Socket> subscriberNodeSocketMap = new ConcurrentHashMap<String, Socket>();

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
		publish(); //thread to broadcast msgs to subscribers

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
						System.out.println("nodeKey:"+ nodeKey);
					
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

	public static void publish() { // publishes/broadcasts to all subscribers

		Thread publish = new Thread() {
			public void run() {
				System.out.println("Starting thread to publish messages to every node");
				while (true) {
					try {
						ObjectMapper objMapper = new ObjectMapper();

						while (inputMessages.size() < 1) {
							Thread.sleep(500);
						}
						DBMessage messageToPublish = inputMessages.poll();
						
						Iterator<Map.Entry<String, Socket> > iterator = subscriberNodeSocketMap.entrySet().iterator(); 
						while (iterator.hasNext()) { 
				            Entry<String, Socket> entry  = iterator.next(); 
				            Socket nodeSocket = entry.getValue();
				            
				            try {
								DataOutputStream dos = new DataOutputStream(nodeSocket.getOutputStream());
								dos.writeUTF(objMapper.writeValueAsString(messageToPublish));

								}catch (Exception e) {
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

	public static DBMessage filterMessages(DBMessage dbMessage) {
		if (dbMessage.getReqType() == RequestType.READ) {
			return dbMessage;
		}
		return null;

	}
	// collect messages at publisher to a queue and keep sending them to subscriber

	public static void listenSource() {

		Thread listen = new Thread() {
			public void run() {
				try {
					pubSourceSocket = new Socket(messageSourceIp, messageSourcePort);
					System.out.println("Started listeining to message source:" + messageSourcePort);

				} catch (IOException e1) {
					e1.printStackTrace();
				}
				// TODO check if this while loop is required
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

						if (inputMessage.getReqType() == RequestType.INSERT
								|| inputMessage.getReqType() == RequestType.DELETE
								|| inputMessage.getReqType() == RequestType.EDIT) {
							inputMessages.add(inputMessage); // this queue broadcasts msgs to all subscribers in the network

						} else if (inputMessage.getReqType() == RequestType.READ) {

							String readTargetNodeId = getNodeWithUpdatedState(inputMessage.getRecordId());
							Socket readTargetNodeSocket = null; // TODO build scoket for the target nodeid
							DataOutputStream dos = new DataOutputStream(readTargetNodeSocket.getOutputStream());
							dos.writeUTF(objMapper.writeValueAsString(inputMessage));
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
						}
						ObjectMapper objMapper = new ObjectMapper();
						String received = dis.readUTF();

						DBMessage inputMessage = objMapper.readValue(received, DBMessage.class);

						if (inputMessage.getReqType() == RequestType.ACK_INSERT
								|| inputMessage.getReqType() == RequestType.ACK_DELETE
								|| inputMessage.getReqType() == RequestType.ACK_EDIT) {
							int Nw = 2; // TODO remove hardcoding
							String requestKey = inputMessage.getMessageKey();
//							System.out.println(Thread.currentThread().getName()+ ": received  message from subscriber node:"+ inputMessage.getSenderId());
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
									populateStateTable(inputMessage.getRecordId(), ackMap.get(requestKey));
									
									flushKeyFromAckMap(requestKey);
									
									// TODO send response to user
								}
							}
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

	public static void populateStateTable(String recordId, Set<String> set) {
		final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
		rwl.writeLock().lock();
		stateTable.put(recordId, set);
		rwl.writeLock().unlock();
		System.out.println("Updated state table with id:" + recordId + "entries:" + set);
	}

	public static String getNodeWithUpdatedState(String recordId) {
		if (stateTable.get(recordId) != null) { // if the record is present in state table return any node in the list
			return stateTable.get(recordId).iterator().next();
		} else { // if record has not been updated at all
					// TODO implement logic to look up in local DB and other DCs
			return null;
		}

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
//		System.out.println("updated ackmap," + ackMapkey + ":" + ackMap.get(ackMapkey));
	}

	public synchronized static void flushKeyFromAckMap(String ackMapkey) {
		final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
		rwl.readLock().lock();
		if (ackMap.get(ackMapkey) != null) {
			rwl.readLock().unlock();
			rwl.writeLock().lock();
			ackMap.remove(ackMapkey);
			rwl.writeLock().unlock();
		}else {
			rwl.readLock().unlock();
		}
		
		System.out.println("flushed key from ackmap," + ackMapkey);
	}

}
