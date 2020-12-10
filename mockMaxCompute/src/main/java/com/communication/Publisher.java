package com.communication;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Publisher {

	static String pubSvrIp = "localhost";
	static String messageSourceIp = "localhost";
	static int pubPort = 5432;
	static int messageSourcePort = 6432;

	static ServerSocket pubSocket;
	static ServerSocket pubSourceSocket;

	static Queue<DBMessage> inputMessages = new LinkedList<DBMessage>();
	public static volatile ConcurrentHashMap<String, Integer> ackMap = new ConcurrentHashMap<String, Integer>(); // no of acknowledgements received for each reqKey


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
		publishToClient();
	}

	public Publisher(int senderPort, int messageSourceConnPort) {
		this.pubPort = senderPort;
		this.messageSourcePort = messageSourceConnPort;
	}

	public static void publishToClient() {
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
						publish(nodeSocket); // once subscription is accepted publish thread keeps running for all
												// subscribers
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		};
		acceptSubscriptions.start();
	}

	public static void publish(final Socket nodeSocket) { // add specification to publish to one particular node socket

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

						DataOutputStream dos = new DataOutputStream(nodeSocket.getOutputStream());

						dos.writeUTF(objMapper.writeValueAsString(messageToPublish));
						System.out.println("Mesage from publisher to subscriber:"
								+ objMapper.writeValueAsString(messageToPublish));

					} catch (Exception e) {
						System.out.println("Exception in publish thread");
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
				Socket pubSourceSocket = null;
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
						System.out.println("received from source:" + received);
						DBMessage inputMessage = objMapper.readValue(received, DBMessage.class);

//						if (filterMessages(inputMessage) != null) {
//							inputMessages.add(inputMessage);
//						}
						if (inputMessage.getReqType() == RequestType.INSERT
								|| inputMessage.getReqType() == RequestType.DELETE
								|| inputMessage.getReqType() == RequestType.EDIT) {
							inputMessages.add(inputMessage); // this queue publishes to all nodes => msg is broadcasted to everyone in the network

						} else if (inputMessage.getReqType() == RequestType.READ) {

							Socket readTargetNodeSocket = getNodeWithUpdatedState(); // TODO get random node or directly
																						// send to rep slave?
							DataOutputStream dos = new DataOutputStream(readTargetNodeSocket.getOutputStream());
							dos.writeUTF(objMapper.writeValueAsString(inputMessage));
							
						} else if (inputMessage.getReqType() == RequestType.ACK_INSERT 
								|| inputMessage.getReqType() == RequestType.ACK_DELETE
								|| inputMessage.getReqType() == RequestType.ACK_EDIT) {
							int Nw = 2; //TODO remove hardcoding
							String requestKey = inputMessage.getRecordId();
							synchronized (this) {
								updateAckMap(requestKey);
								int nAcks = ackMap.get(requestKey);
								System.out.println("nAcks:" + nAcks + " nAcksReqd:" + Nw);
							    if (nAcks >= Nw) {
							    	/**
									 * once the request is successful flush the ack entry from map to enable
									 * processing request on same entry by same client
									 **/
									flushKeyFromAckMap(requestKey);
									//TODO send message to user or DB?
							    }
							    
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

	public static Socket getNodeWithUpdatedState() {
		// map of Sockets and
		return null;

	}
	private synchronized static void updateAckMap(String ackMapkey) {
		final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
		rwl.readLock().lock();
		if (ackMap.get(ackMapkey) != null) {
			rwl.readLock().unlock();
			rwl.writeLock().lock();
			ackMap.put(ackMapkey, ackMap.get(ackMapkey) + 1);
			rwl.writeLock().unlock();
		} else {
			rwl.readLock().unlock();
			rwl.writeLock().lock();
			ackMap.put(ackMapkey, 1);
			rwl.writeLock().unlock();
		}
		System.out.println("updated ackmap," + ackMapkey + ":" + ackMap.get(ackMapkey));
	}
	
	private synchronized static void flushKeyFromAckMap(String ackMapkey) {
		if (ackMap.get(ackMapkey) != null) {
			ackMap.remove(ackMapkey);
		}
		System.out.println("flushed key from ackmap," + ackMapkey);
	}

}
