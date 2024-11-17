package Nodes;


import shared.Message;
import shared.MessageQueue;
import shared.OPERATION;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import Services.AckServiceServer;
import remote.messageQueueServer;

import java.rmi.RemoteException;
import java.security.Key;


public class Node extends Thread {
    private final String nodeId;
    private final UUID UID;
    private final GossipNode gossipNode;  
    ConcurrentHashMap<UUID, Integer> knownNodes = new ConcurrentHashMap<>();  // Known node IDs with their UDP ports

    
    private final ArrayList<String> documentsList = new ArrayList<>(); 
    private boolean isLeader;
    private messageQueueServer messageQueue; 
    private AckServiceServer ackS = null;

    private volatile boolean running = true;
    
    @Override
    public void run() {
        try {
            if(isLeader)
            {
                System.out.println("Leader thread started for node: " + nodeId);
                // startRMIService();
                // startACKService();
                startLeaderServices();
                
                
            }

            while(running){
                try {
                    System.out.println("Leader thread is running: " + isLeader);

                    if (isLeader ) {
                        System.out.println("\n\tKNOWN NODESS:\n");
                        System.out.print("\t");
                        System.out.println(knownNodes);
                        System.out.print("\n");

                        System.out.println("Checking if this keeps printing or not");
                        System.out.println("Checking queue status: " + checkQueue());
                        // things only leader will be abvle to do like commits TBD
                        if (checkQueue()) {
                            System.out.println("there are messages in queue");
                            MessageQueue mq = messageQueue.getQueue();
                            Message s = mq.dequeue();
                            System.out.println("PROCESSING the message");
                            System.out.println(s);

                            //  FUNCTION TO PROCESS MESSAGES FROM QUEUE
                        }
                           
                      
                    }
    
                    Thread.sleep(1000);
                
                } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); // Preserve interrupt status
                        System.out.println("Worker thread interrupted.");
                        System.out.println("Node " + nodeId + " interrupted.");
                        break;
                } 
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }

    public Node(String nodeId) throws RemoteException {
        this.nodeId = nodeId;
        this.UID =  UUID.randomUUID();;
        this.gossipNode = new GossipNode(this);  // Initialize gossip component by passing 'this' node to 'gossipnode'
        this.isLeader = false;
        //this.messageQueue = new messageQueueServer() ;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            cleanupOnShutdown();
        }));
        //messageQueue = new messageQueueServer(nodeId, 2323);
        gossipNode.start();
    }
    public Node(String nodeId, boolean L) throws RemoteException  {
        this.nodeId = nodeId;
        this.UID =  UUID.randomUUID();;
        this.gossipNode = new GossipNode(this);  
        this.isLeader = L;
        //this.messageQueue = new messageQueueServer() ;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            cleanupOnShutdown();
        }));
        //messageQueue = new messageQueueServer(nodeId, 2323);
        gossipNode.start();
    }

    public synchronized  boolean checkQueue() throws RemoteException{
        if (messageQueue != null){return  messageQueue.checkQueue();}
        else return false;
    }

    public void stopRunning() {
        running = false; 
    }

    private void cleanupOnShutdown() {
        System.out.println("Node " + this.nodeId + " shutting down.");
    }

    public UUID getNodeId() {
        return UID;
    }
    public String getNodeName(){
        return nodeId;
    }

    public GossipNode getGossipNode() {
        return gossipNode;
    }


    public void addKnownNode(UUID nodeId, int port){
        knownNodes.put(nodeId,  port);
    }

    public void startLeaderServices() throws RemoteException {
        startRMIService();       
        startACKService();    
    }
    public void startRMIService() throws RemoteException {
        try {
            messageQueue = new messageQueueServer(nodeId, 2323);
            messageQueue.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void startACKService() {
        new Thread(() -> {
            try {
            if (ackS == null){
            ackS = new AckServiceServer();
            ackS.startServer();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        }).start();
    }

    // public void stopRMIService() {
    //     try {
    //         Naming.unbind("rmi://localhost/" + nodeId);
    //         UnicastRemoteObject.unexportObject(this, true);  // Unexport the remote object
    //         System.out.println("Node " + nodeId + " unregistered from RMI.");
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //     }
    // }
        
    
    // Provides a list of known nodes for gossiping
    public List<Map.Entry<UUID, Integer>> getRandomNodes() {
        List<Map.Entry<UUID, Integer>> nodesList = new ArrayList<>(knownNodes.entrySet());
        Collections.shuffle(nodesList);
        return nodesList.subList(0, Math.min(3, nodesList.size())); 
    }

    public boolean isLeader(){
        return isLeader;
    }



    public void processMessage(Message msg){
        OPERATION op = msg.getOperation();
    }

}