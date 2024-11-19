package Nodes;


import shared.Message;
import shared.MessageQueue;
import shared.OPERATION;
import utils.UniqueIdGenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import Resources.Document;
import Services.AckServiceServer;
import remote.messageQueueServer;

import java.rmi.RemoteException;
import java.security.Key;


public class Node extends Thread {
    private final String nodeId;
    private final UUID UID;
    private final GossipNode gossipNode;  
    private ConcurrentHashMap<UUID, Integer> knownNodes = new ConcurrentHashMap<>();  // Known node IDs with their UDP ports
    private ArrayList<Document> documentsList = new ArrayList<>(); 
    private ArrayList<String> operationsBatch = new ArrayList<>();
    private boolean isLeader;
    private messageQueueServer messageQueue; 
    private AckServiceServer ackS = null;
    private ConcurrentHashMap<UUID, String> documentChangesACKS = new ConcurrentHashMap<>();  // to save acks for operations of syncing before commmit
    private ConcurrentHashMap<String, String> distributedOperations = new ConcurrentHashMap<>();  // to save BIG SCALE operationsID WITH ITS STATUS
    private int quorum;

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
                            //process everything in queue
                            MessageQueue mq = messageQueue.getQueue();
                            while (!mq.isEmpty()){
                                Message s = mq.dequeue();
                                System.out.println("PROCESSING the message");
                                System.out.println(s);
                                processMessage(s);
                            }
                            startSyncProcess();
                            
                            //  FUNCTION TO PROCESS MESSAGES FROM QUEUE
                        }
                        if(!distributedOperations.isEmpty()){
                            System.out.println("Distributed Operations Status:");
                            System.out.println("\n\t\t" + distributedOperations);
                        }
                           
                      
                    }
                    System.out.println(this.getGossipNode().getHeartbeatService().toString());
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
        this.quorum=0;
        //this.messageQueue = new messageQueueServer() ;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            cleanupOnShutdown();
        }));
        //messageQueue = new messageQueueServer(nodeId, 2323);
        gossipNode.start();
    }
    private synchronized void updateQuorum(){
        long N = knownNodes.mappingCount();
        this.quorum= ((int)N / 2) + 1;
    }
    private int getQuorum(){
       return this.quorum;
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

    public void addACK(UUID nodeId, String syncOP){
        documentChangesACKS.putIfAbsent(nodeId, syncOP);
    }

    public void startLeaderServices() throws RemoteException {
        startRMIService();       
        //startACKService();    
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


    public synchronized boolean addDocument(Document doc){
        return documentsList.add(doc);
    }
    public synchronized boolean updateDocument(int index, Document doc){
        try{
            documentsList.set(index, doc);
            return true;
        }catch(Exception e){
            e.printStackTrace();
            return false;
        }
    }
    public synchronized boolean updateDocument(Document doc){
        for (int i = 0; i < documentsList.size(); i++) {
            if (documentsList.get(i).getId().equals(doc.getId())) {
                documentsList.set(i, doc);
                System.out.println("Document updated: " + doc);
                return true;
            }
        }
        System.out.println("Document not found for update: " + doc);
        return false;
    }
    protected int findDocumentIndex(UUID id) {
        for (int i = 0; i < documentsList.size(); i++) {
            if (documentsList.get(i).getId().equals(id)) {
                return i;
            }
        }
        return -1; // Return -1 if not found
    }
    public synchronized boolean removeDocument(Document document){
        return documentsList.removeIf(doc -> doc.getId().equals(document.getId()));
    }
    public synchronized void addOperation(String op){
        operationsBatch.add(op);
    }

    public synchronized void processMessage(Message msg){
        OPERATION op = msg.getOperation();
        Object payload = msg.getPayload();
        System.out.println("\n\t" + op);
        System.out.println("\n\t" + payload);

        try{
            Document document = Document.clone((Document)payload);
            switch (op) {
                case CREATE:
                    if (!documentsList.contains(document)) {
                        addDocument(document);
                        addOperation("CREATE" + ";" + document.toString());
                        System.out.println("Document created: " + document);
                    } else {
                        System.out.println("Document already exists: " + document);
                    }
                    break;
    
                case UPDATE:
                    boolean updated = false;
                    for (int i = 0; i < documentsList.size(); i++) {
                        Document existingDoc = documentsList.get(i);
                        if (existingDoc.getId().equals(document.getId())) {
                            if (existingDoc.getVersion() < document.getVersion()) {
                                updateDocument(i, document);
                                addOperation("UPDATE" + ";" + document.toString());
                                System.out.println("Document updated to latest version: " + document);
                            } else {
                                System.out.println("Document already up-to-date: " + existingDoc);
                            }
                            updated = true;
                            break;
                        }
                    }
                    if (!updated) {
                        System.out.println("Document not found for update, adding it: " + document);
                        addDocument(document);
                        addOperation("CREATE" + ";" + document.toString());
                    }
                    break;
    
                case DELETE:
                    boolean removed = removeDocument(document);
                    if (removed) {
                        addOperation("DELETE" + ";" + document.toString());
                        System.out.println("Document deleted: " + document);
                    } else {
                        System.out.println("Document not found for deletion: " + document);
                    }
                    break;
    
                default:
                    System.err.println("Unsupported operation: " + op);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public ArrayList<Document> getDocumentsList() {
        return documentsList;
    }

     

    /*
   _____                  
  / ____|                 
 | (___  _   _ _ __   ___ 
  \___ \| | | | '_ \ / __|
  ____) | |_| | | | | (__ 
 |_____/ \__, |_| |_|\___|
          __/ |           
         |___/            
     */
    private synchronized void addDistributedOperation(String op){
        distributedOperations.putIfAbsent(op, "WAITING");
        //distributedOperations.put(op, "WAITING");
    }
    private synchronized void commitDistributedOperation(String op){
        distributedOperations.replace(op, "FINISHED");
        //distributedOperations.put(op, "WAITING");
    }
    public void startSyncProcess(){
        updateQuorum();
        try{
        
            String operationId = UniqueIdGenerator.generateOperationId((Integer.toString(operationsBatch.hashCode())));
            addDistributedOperation(operationId);
        
            Message syncMessage = new Message(
                OPERATION.SYNC,      // WILL JOIN ALL OPERATIONS IN ARRAT TO THE MESSAGE
                operationId + ";" +getNodeId() + ":" +this.gossipNode.getHeartbeatService().getUDPport() +";" 
                + String.join("$", operationsBatch) 
            );
            gossipNode.getHeartbeatService().broadcast(syncMessage, true);
            System.out.println("SYNC message sent with operation ID: " + operationId);
            waitForQuorum(operationId);
        
            /*  OPERATION BY OPERATION INSTEAD OF BATCH OF THEM

            for (String operation : operationsBatch) {
            String operationId = UniqueIdGenerator.generateOperationId(Integer.toString(operation.hashCode()));
            addDistributedOperation(operationId);
            Message syncMessage = new Message(
                OPERATION.SYNC,
                operationId + ";" + operation 
            );
            gossipNode.getHeartbeatService().broadcast(syncMessage, true);
            System.out.println("SYNC message sent for operation: " + operation + " with operation ID: " + operationId);
            waitForQuorum(operationId);
        }
    //*/
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    private void waitForQuorum(String operationId) {
        new Thread(() -> {
            int attempts = 10; // Max attempts to wait for quorum
            int delay = 1000; // 1 second between checks
    
            try {
                while (attempts > 0) {
                    long ackCount = documentChangesACKS.values().stream()
                        .filter(op -> op.equals(operationId))
                        .count();
    
                    if (ackCount >= getQuorum()) {
                        System.out.println("\n\n\n Quorum achieved for operation ID: " + operationId+ "n\n\n");
                        commitSyncProcess(operationId);
                        
                        break;
                    }
    
                    Thread.sleep(delay);
                    attempts--;
                }
    
                if (attempts == 0) {
                    System.err.println("Failed to achieve quorum for operation ID: " + operationId);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        }).start();
    }
    public void commitSyncProcess(String operationId){
        try{
            sendCommitMessage(operationId);
            System.out.println("COMMIT message sent FOR operation ID: " + operationId);
            clearOperationsBatch();
            commitDistributedOperation(operationId);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    private void sendCommitMessage(String operationID){
        Message syncMessage = new Message(
                OPERATION.COMMIT,    
                operationID
            );
            gossipNode.getHeartbeatService().broadcast(syncMessage, false);
    }
    private void clearOperationsBatch(){
        operationsBatch.clear();
    }

    // Create updated DB in new Node
    public Message startFullSyncProcess(){
        Message fullSyncContent = new Message(OPERATION.FULL_SYNC_ANS, documentsList);
        return fullSyncContent;
    }
}