package Nodes;
import Documents.*;
import remote.DocumentManagementInterface;
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
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class Node extends Thread implements DocumentManagementInterface{
    private final String nodeId;
    private final UUID UID;
    private final GossipNode gossipNode;  // Handles gossip protocol aspects of this node
    private final Set<String> knownNodes = new HashSet<>();  // Known node IDs (dynamic list)
    private final MessageQueue messageQueue = new MessageQueue();
    private boolean isLeader;
    
        private static final String DISCOVERY_SERVER_HOST = "localhost";
        private static final int DISCOVERY_SERVER_PORT = 9090;
    
        private volatile boolean running = true;
    
        private Map<String, Document> documents = new ConcurrentHashMap<>();  // Document storage
        private final Map<String, AtomicInteger> ackCount = new ConcurrentHashMap<>();  // Tracks ACK counts per version
    
    
    
        @Override
        public void run() {
            try{
            registerWithDiscoveryServer();
            if (isLeader) {
                startRMIService();  // Start RMI initially if this node is the leader
            }
            while(running){
                try {
                    if (isLeader) {
                        // computate logic to check wether there are documetns to commit to the rest of the nodes 
                        for (Document doc : documents.values()) {
                            propagateDocumentVersion(doc);  // Leader propagates document versions
                        }
                    }

                    Thread.sleep(1000);
            
                } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); // Preserve interrupt status
                        System.out.println("Worker thread interrupted.");
                        System.out.println("Node " + nodeId + " interrupted.");
                        break;
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            }

            }  catch (RemoteException e) {
                e.printStackTrace();
            }
            
    
        }
    
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
        public void setLeader(boolean isLeader) {
            this.isLeader = isLeader;
        if (isLeader) {
            System.out.println("Node " + nodeId + " is now the leader.");
            try {
                startRMIService();  // Start RMI when node becomes leader
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Node " + nodeId + " is no longer the leader.");
            stopRMIService();  // Stop RMI when node is no longer the leader
        }
    }


    public Node(String nodeId) throws RemoteException {
        this.nodeId = nodeId;
        this.UID =  UUID.randomUUID();;
        this.gossipNode = new GossipNode(this);  // Initialize gossip component by passing 'this' node to 'gossipnode'
        this.documents = new HashMap<>();
        this.isLeader = false;

        // Register with discovery server
        registerWithDiscoveryServer();

        // Periodically update the list of known nodes
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                updateKnownNodes();
            }
        }, 0, 5000);  // Update every 5 seconds
        // Register the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            cleanupOnShutdown();
        }));
        // Start the gossip protocol
        gossipNode.start();
    }
    public Node(String nodeId, boolean L) {
        this.nodeId = nodeId;
        this.UID =  UUID.randomUUID();;
        this.gossipNode = new GossipNode(this);  // Initialize gossip component by passing 'this' node to 'gossipnode'
        this.documents = new HashMap<>();
        this.isLeader = L;
        // Register with discovery server
        registerWithDiscoveryServer();

        // Periodically update the list of known nodes
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                updateKnownNodes();
            }
        }, 0, 5000);  // Update every 5 seconds
        // Register the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            cleanupOnShutdown();
        }));
        // Start the gossip protocol
        gossipNode.start();
    }

    public void stopRunning() {
        running = false; 
    }

    private void cleanupOnShutdown() {
        this.unregisterFromDiscoveryServer();
        if (isLeader) {
            stopRMIService();  // Ensure RMI is unregistered on shutdown if the node is a leader
        }
        System.out.println("Node " + this.nodeId + " shutting down.");
    }

    public String getNodeId() {
        return nodeId;
    }

    public GossipNode getGossipNode() {
        return gossipNode;
    }

    private void registerWithDiscoveryServer() {
        try (Socket socket = new Socket(DISCOVERY_SERVER_HOST, DISCOVERY_SERVER_PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            out.println("REGISTER " + nodeId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void unregisterFromDiscoveryServer() {
        try (Socket socket = new Socket(DISCOVERY_SERVER_HOST, DISCOVERY_SERVER_PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            out.println("UNREGISTER " + nodeId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updateKnownNodes() {
        try (Socket socket = new Socket(DISCOVERY_SERVER_HOST, DISCOVERY_SERVER_PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            out.println("GET_NODES");
            String line;
            System.out.println("Node: " + nodeId + " updating known nodes");
            while (!(line = in.readLine()).equals("END")) {
                String[] parts = line.split(" ");
                String peerNodeId = parts[0];
                if (!peerNodeId.equals(nodeId)) {  // Ignore self in known nodes
                    knownNodes.add(peerNodeId);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Provides a list of known nodes for gossiping
    public List<String> getRandomNodes() {
        List<String> nodesList = new ArrayList<>(knownNodes);
        Collections.shuffle(nodesList);
        return nodesList.subList(0, Math.min(3, nodesList.size()));  // Select up to 3 random nodes
    }

    public boolean isLeader(){
        return isLeader;
    }


// RMI SECTION
public void propagateDocumentVersion(Document doc) throws RemoteException {
    if (isLeader) {
        System.out.println("Leader " + nodeId + " propagating document version " + doc.getVersionNumber());
        gossipNode.gossipDocument(doc);
        ackCount.put(doc.getId(), new AtomicInteger(0));  // Initialize ACK count for quorum
    }
}

@Override
public void receiveDocument(Document doc) throws RemoteException {
    if (!documents.containsKey(doc.getId())) {
        System.out.println("Node " + nodeId + " received document version " + doc.getVersionNumber());
        documents.put(doc.getId(), doc);
        sendAckToLeader(doc);  // Send ACK back to the leader
        gossipNode.gossipDocument(doc);  // Continue gossiping the document
    }
}

@Override
public void sendAckToLeader(Document doc) throws RemoteException {
    if (!isLeader) {
        try {
            DocumentManagementInterface leader = (DocumentManagementInterface) Naming.lookup("rmi://localhost/Node-1");
            leader.processAck(doc);
            System.out.println("Node " + nodeId + " sent ACK for document version " + doc.getVersionNumber());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

public void processAck(Document doc) throws RemoteException {
    if (isLeader) {
        int ackCountForDoc = ackCount.get(doc.getId()).incrementAndGet();
        int quorum = (knownNodes.size() / 2) + 1;

        if (ackCountForDoc >= quorum) {
            System.out.println("Leader " + nodeId + " received quorum for document " + doc.getId());
            commitDocumentVersion(doc);
        }
    }
}

private void commitDocumentVersion(Document doc) throws RemoteException {
    System.out.println("Leader " + nodeId + " committing document " + doc.getId());
    gossipNode.gossipCommit(doc);
}

@Override
public void applyCommittedVersion(Document doc) throws RemoteException {
    System.out.println("Node " + nodeId + " applying committed version " + doc.getVersionNumber());
    documents.put(doc.getId(), doc);  // Store as committed version
}


}