package Nodes;


import shared.Message;
import shared.MessageQueue;


import java.util.ArrayList;
import java.util.Collections;

import java.util.HashSet;
import java.util.List;

import java.util.Set;

import java.util.UUID;

import Services.AckServiceServer;
import remote.messageQueueServer;

import java.rmi.RemoteException;


public class Node extends Thread {
    private final String nodeId;
    private final UUID UID;
    private final GossipNode gossipNode;  
    private final Set<UUID> knownNodes = new HashSet<>();  // Known node IDs (dynamic list)

    
    private final ArrayList<String> documentsList = new ArrayList<>();
    private boolean isLeader;
    private messageQueueServer messageQueue; 
    private AckServiceServer ackS;

    private volatile boolean running = true;
    
    @Override
    public void run() {
        try {
            if(isLeader)
            {
                System.out.println("Leader thread started for node: " + nodeId);
                startRMIService();
                
                
            }

            while(running){
                try {
                    System.out.println("Leader thread is running: " + isLeader);

                    if (isLeader ) {
                        System.out.println("Checking if this keeps printing or not");
                        System.out.println("Checking queue status: " + checkQueue());
                        // things only leader will be abvle to do like commits TBD
                        if (checkQueue()) {
                            System.out.println("there are messages in queue");
                            MessageQueue mq = messageQueue.getQueue();
                            Message s = mq.dequeue();
                            System.out.println("priting the message");
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

    public GossipNode getGossipNode() {
        return gossipNode;
    }
    public void addKnownNode(UUID nodeId){
        knownNodes.add(nodeId);
    }

    public void startRMIService() throws RemoteException {
        try {
            messageQueue = new messageQueueServer(nodeId, 2323);
            messageQueue.start();
            ackS = new AckServiceServer();
            ackS.startServer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
/*
    public void stopRMIService() {
        try {
            Naming.unbind("rmi://localhost/" + nodeId);
            UnicastRemoteObject.unexportObject(this, true);  // Unexport the remote object
            System.out.println("Node " + nodeId + " unregistered from RMI.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
        */
    
    // Provides a list of known nodes for gossiping
    public List<UUID> getRandomNodes() {
        List<UUID> nodesList = new ArrayList<>(knownNodes);
        Collections.shuffle(nodesList);
        return nodesList.subList(0, Math.min(3, nodesList.size())); 
    }

    public boolean isLeader(){
        return isLeader;
    }



}