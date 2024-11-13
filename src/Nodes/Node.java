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

import java.rmi.RemoteException;


public class Node extends Thread {
    private final String nodeId;
    private final UUID UID;
    private final GossipNode gossipNode;  
    private final Set<String> knownNodes = new HashSet<>();  // Known node IDs (dynamic list)

    //private final MessageQueue messageQueue = new MessageQueue() ;
    private final ArrayList<String> documetnsList = new ArrayList<>();
    private boolean isLeader;
    
    private volatile boolean running = true;

    @Override
    public void run() {

        while(running){
            try {
                if (isLeader) {
                    // things only leader will be abvle to do like commits TBD
                }

                Thread.sleep(1000);
            
            } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Preserve interrupt status
                    System.out.println("Worker thread interrupted.");
                    System.out.println("Node " + nodeId + " interrupted.");
                    break;
            } 
        }
    }

    public Node(String nodeId) {
        this.nodeId = nodeId;
        this.UID =  UUID.randomUUID();;
        this.gossipNode = new GossipNode(this);  // Initialize gossip component by passing 'this' node to 'gossipnode'
       // this.messageQueue =  new MessageQueue(); 
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            cleanupOnShutdown();
        }));

        gossipNode.start();
    }
    public Node(String nodeId, boolean L)  {
        this.nodeId = nodeId;
        this.UID =  UUID.randomUUID();;
        this.gossipNode = new GossipNode(this);  
        this.isLeader = L;
        //this.messageQueue =  new MessageQueue(); 
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            cleanupOnShutdown();
        }));

        gossipNode.start();
    }

    public void stopRunning() {
        running = false; 
    }

    private void cleanupOnShutdown() {
        System.out.println("Node " + this.nodeId + " shutting down.");
    }

    public String getNodeId() {
        return nodeId;
    }

    public GossipNode getGossipNode() {
        return gossipNode;
    }
    public void addKnownNode(String nodeId){
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
    public List<String> getRandomNodes() {
        List<String> nodesList = new ArrayList<>(knownNodes);
        Collections.shuffle(nodesList);
        return nodesList.subList(0, Math.min(3, nodesList.size())); 
    }

    public boolean isLeader(){
        return isLeader;
    }



}