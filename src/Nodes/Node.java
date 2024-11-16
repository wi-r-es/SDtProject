package Nodes;


import shared.MessageQueue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.UUID;

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

    private volatile boolean running = true;
    
    @Override
    public void run() {
        try {
            if(isLeader)
            {messageQueue.start();}

            while(running){
                try {
                    if (isLeader) {
                        // things only leader will be abvle to do like commits TBD
                        if(checkQueue()){
                            System.out.println("there are messages in queue");
                            MessageQueue mq = messageQueue.getQueue();
                            String s = mq.dequeue();
                            System.out.println("priting the message");
                            System.out.println(s);
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
        //this.messageQueue = new messageQueueServer() ;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            cleanupOnShutdown();
        }));
        messageQueue = new messageQueueServer(nodeId, 2323);
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
        messageQueue = new messageQueueServer(nodeId, 2323);
        gossipNode.start();
    }

    public boolean checkQueue(){
        return messageQueue.checkQueue();
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
/*
    public void startRMIService() throws RemoteException {
        try {
            UnicastRemoteObject.exportObject(this, 0);  // Export with any available port
            Naming.rebind("rmi://localhost/" + nodeId, this);
            System.out.println("Node " + nodeId + " registered with RMI as leader.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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