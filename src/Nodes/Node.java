package Nodes;


import shared.Message;
import shared.MessageQueue;
import shared.OPERATION;
import utils.UniqueIdGenerator;
import utils.PrettyPrinter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
    private ConcurrentHashMap<UUID, String> knownNodesNames = new ConcurrentHashMap<>();  // Known node IDs with their name
    //private ArrayList<Document> documentsList = new ArrayList<>(); 
    private DocumentsDB documents = new DocumentsDB();
    //private ArrayList<Document> temp = null;
    private ArrayList<String> operationsBatch = new ArrayList<>();
    private boolean isLeader;
    private messageQueueServer messageQueue; 
    private AckServiceServer ackS = null;
    private ConcurrentHashMap<UUID, String> documentChangesACKS = new ConcurrentHashMap<>();  // to save acks for operations of syncing before commmit
    private ConcurrentHashMap<String, String> distributedOperations = new ConcurrentHashMap<>();  // to save BIG SCALE operationsID WITH ITS STATUS
    private ConcurrentHashMap<String, ArrayList<String>> distributedOperationsDescription = new ConcurrentHashMap<>();  
    private ConcurrentHashMap<String, String> distributedOperationsDesignation = new ConcurrentHashMap<>();  
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
                    System.out.println(this.getGossipNode().getHeartbeatService().toString());
                    if (isLeader ) {
                        System.out.println("Checking queue status: " + checkQueue());
                        if (checkQueue()) {
                            //System.out.println("there are messages in queue");
                            processAndCommit();
                        }
                        if(!distributedOperations.isEmpty()){
                            System.out.println("Distributed Operations Status:");
                            System.out.println("\n\t\t" + distributedOperations);
                            for( String op : distributedOperations.keySet()){
                                System.out.println("\n" + distributedOperationsDescription.get(op));
                            }
                            
                        }
                           
                      
                    }
                    System.out.println("\n\n\n"+this.getGossipNode().getHeartbeatService().toString());
                    System.out.println("\tGET NODES LIST: \n" );
                    printKnownNodes();
                    
                    
                    System.out.println("\n\n\n"+this.getGossipNode().getHeartbeatService().toString());
                    System.out.println("\tGET DOCUMENT LIST: \n" + getDocuments().getDocumentsMap().toString() + this.getGossipNode().getHeartbeatService().toString());
                    System.out.println("\n\n\nIS IT EMPTY: \n" + getDocuments().getDocumentsMap().isEmpty());
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
    public void addKnownNode(UUID nodeId, String name){
        knownNodesNames.putIfAbsent(nodeId,  name);
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
    public List<Map.Entry<UUID, Integer>> getNodesList() {
        List<Map.Entry<UUID, Integer>> nodesList = new ArrayList<>(knownNodes.entrySet());
        return nodesList; 
    }

    public boolean isLeader(){
        return isLeader;
    }

    private synchronized void printKnownNodes(){
        List<Map.Entry<UUID, Integer>> knownNodes = getNodesList();
        String[] headers = {"UUID", "Value"};
        List<String[]> rows = knownNodes.stream()
                .map(entry -> new String[]{entry.getKey().toString(), entry.getValue().toString()})
                .collect(Collectors.toList());

        // Print the table
        PrettyPrinter.printTable(headers, rows);
    }


   
    public DocumentsDB getDocuments(){
        return documents;
    }
    
    


/*
███    ███ ███████  ██████       ██████  ██    ██ ███████ ██    ██ ███████     
████  ████ ██      ██           ██    ██ ██    ██ ██      ██    ██ ██          
██ ████ ██ ███████ ██   ███     ██    ██ ██    ██ █████   ██    ██ █████       
██  ██  ██      ██ ██    ██     ██ ▄▄ ██ ██    ██ ██      ██    ██ ██          
██      ██████████  ██████       ██████   ██████  ███████  ██████  ███████     
                                    ▀▀                                         
███████ ██████   ██████   ██████ ███████ ███████ ███████ ██ ███    ██  ██████  
██   ██ ██   ██ ██    ██ ██      ██      ██      ██      ██ ████   ██ ██       
██████  ██████  ██    ██ ██      █████   ███████ ███████ ██ ██ ██  ██ ██   ███ 
██      ██   ██ ██    ██ ██      ██           ██      ██ ██ ██  ██ ██ ██    ██ 
██      ██   ██  ██████   ██████ ███████ ███████ ███████ ██ ██   ████  ██████  
                                                                               
                                                                              
*/   

    public synchronized void addOperation(String op){
        operationsBatch.add(op);
    }

    public synchronized void processMessage(Message msg){
        OPERATION op = msg.getOperation();
        Object payload = msg.getPayload();
        System.out.println("\n\t" + op);
        System.out.println("\n\t" + payload);
        Document document;
        try {
            document = Document.clone((Document)payload);
            processOP(op, document);
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        
    }
    protected synchronized void processOP(OPERATION op, Document document){
        try{

            switch (op) {
                case CREATE:
                    if (documents.updateOrAddDocument(document)) {
                        addOperation("CREATE" + ";" + document.toString());
                        System.out.println("Document created: " + document);
             
                    } 
                    break;
    
                case UPDATE:
                    if (documents.updateOrAddDocument(document)) {
                       addOperation("UPDATE;" + document.toString());
           
                    }
                    break;
    
                case DELETE:
                    if (documents.removeDocument(document)) {
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
    protected synchronized void processOP(String op, Document document){
        try{
            System.out.println("inside proccessOP for syncing->" + op + ":"+document  );
            switch (op) {
                case "CREATE":
                    if (documents.updateOrAddDocument(document)) {
                        //addOperation("CREATE" + ";" + document.toString());
                        System.out.println("Node" + this.nodeId + " Document created: " + document);
         
                    } 
                    break;
    
                case "UPDATE":
                    if (documents.updateOrAddDocument(document)) {
                        //addOperation("UPDATE;" + document.toString());
                        System.out.println("Document not found for deletion: " + document);
                    
                    }
                    break;
    
                case "DELETE":
                    if (documents.removeDocument(document)) {
                        //addOperation("DELETE" + ";" + document.toString());
                        System.out.println("Node" + this.nodeId + "Document deleted: " + document);
                  
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



    

    public void debugState(String context) {
        System.out.println("DocumentsDB state at " + context + ": " + documents.getDocuments().toString());
    }   

    /*
███████ ██    ██ ███    ██  ██████ 
██       ██  ██  ████   ██ ██      
███████   ████   ██ ██  ██ ██      
     ██    ██    ██  ██ ██ ██      
███████    ██    ██   ████  ██████                                           
     */
    private synchronized void addDistributedOperation(String op){
        distributedOperations.putIfAbsent(op, "WAITING");
        //distributedOperations.put(op, "WAITING");
    }
    private synchronized void commitDistributedOperation(String op){
        distributedOperations.replace(op, "COMMITED");
        //distributedOperations.put(op, "WAITING");
    }
    private synchronized void cancelDistributedOperation(String op){
        distributedOperations.replace(op, "CANCELED");
        Message syncMessage = new Message(
                OPERATION.REVERT,     
                ";" +getNodeId() + ":" +this.gossipNode.getHeartbeatService().getUDPport() +";" 
            );
            gossipNode.getHeartbeatService().broadcast(syncMessage, true);
        System.out.println("Operation canceled");
        //distributedOperations.put(op, "WAITING");
    }
    private synchronized boolean isCommited(String op){
        String state = distributedOperations.get(op);
        return state == "FINISHED" ? true : false; 
    }
    private synchronized boolean isCancelled(String op){
        String state = distributedOperations.get(op);
        return state == "CANCELED" ? true : false; 
    }
    public void processAndCommit() {
        documents.lock();
        try {
            documents.createTempMap();
    
            // Process messages from the queue
            MessageQueue mq = messageQueue.getQueue();
            while (!mq.isEmpty()) {
                Message s = mq.dequeue();
                System.out.println("PROCESSING the message");
                System.out.println(s);
                processMessage(s);
            }
            String opID = startSyncProcess();
            debugState("BEFORE COMMIT");
            // Check if operation was commited
            if (opID != null &&  isCommited(opID)) {
                System.out.println("OPERATION COMMITED");
                documents.commitChanges();
                debugState("AFTER COMMIT");
            } else if(opID != null && isCancelled(opID)) {
                System.err.println("Quorum not achieved. Reverting changes.");
                documents.revertChanges();
            }
        } catch (RemoteException e) {
                e.printStackTrace();
        } finally {
            documents.unlock();
        }
    }
    private String startSyncProcess(){
        updateQuorum();
        try{
            
            String operationId = UniqueIdGenerator.generateOperationId((Integer.toString(operationsBatch.hashCode())));
            System.out.println("Operations Batch for sync");
            System.out.println(operationsBatch);
            addDistributedOperation(operationId);
            distributedOperationsDescription.put(operationId, operationsBatch);
            distributedOperationsDesignation.put(operationId, "SYNC");
            //Filter duplicates in operationsBatch
            List<String> uniqueOperations = operationsBatch.stream().distinct().toList();
        
            Message syncMessage = new Message(
                OPERATION.SYNC,      // WILL JOIN ALL OPERATIONS IN ARRAT TO THE MESSAGE
                operationId + ";" +getNodeId() + ":" +this.gossipNode.getHeartbeatService().getUDPport() +";" 
                + String.join("$", uniqueOperations) 
            );
            gossipNode.getHeartbeatService().broadcast(syncMessage, true);
            System.out.println("SYNC message sent with operation ID: " + operationId);
            CompletableFuture<Boolean> quorumFuture = waitForQuorum(operationId);

        quorumFuture.thenAccept(success -> {
            if (success) {
                commitSyncProcess(operationId);
                System.out.println("Commit successful for operation ID: " + operationId);
            } else {
                //System.err.println("Retrying sync process for operation ID: " + operationId);
                retrySyncProcess(operationId); 
            }
        }).exceptionally(e -> {
            System.err.println("\n\n\n\n\n\n\tERROR FATAL IN SYNC NIM HEREHERHEHRE\n\n\n\n\n\n");
            e.printStackTrace();
            return null;
        });
        return operationId;
        
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
            */
        }catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    /** 
                                                                
                                                            ██████  ██    ██  ██████  ██████  ██    ██ ███    ███ 
                                                            ██    ██ ██    ██ ██    ██ ██   ██ ██    ██ ████  ████ 
                                                            ██    ██ ██    ██ ██    ██ ██████  ██    ██ ██ ████ ██ 
                                                            ██ ▄▄ ██ ██    ██ ██    ██ ██   ██ ██    ██ ██  ██  ██ 
                                                            ██████   ██████   ██████  ██   ██  ██████  ██      ██ 
                                                                ▀▀                                                
                                                                
    */


    private synchronized void updateQuorum(){
        long N = knownNodes.mappingCount();
        this.quorum= ((int)N / 2) + 1;
    }
    private int getQuorum(){
       return this.quorum;
    }                                                                
    private CompletableFuture<Boolean> waitForQuorum(String operationId) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        new Thread(() -> {
            
            AtomicInteger attempts = new AtomicInteger(10); // Max attempts to wait for quorum
            int delay = 500; // Sleep times between checks
    
            try {
                while (attempts.get() > 0) {
                    long ackCount = documentChangesACKS.values().stream()
                        .filter(op -> op.equals(operationId))
                        .count();
    
                    if (ackCount >= getQuorum()) {
                        System.out.println("\n\n\n Quorum achieved for operation ID: " + operationId+ "n\n\n");
                        //commitSyncProcess(operationId);
                        future.complete(true); 
                        return;
                    }

                    System.out.println("[DEBUG] Nodes Missing ACKs for Operation ID: " + operationId);
                    knownNodes.forEach((nodeId, port) -> {
                        if (!documentChangesACKS.containsKey(nodeId)) {
                            System.out.println("[DEBUG] Missing ACK from Node: " + nodeId);
                        }
                    });
    
                    Thread.sleep(delay);
                    attempts.decrementAndGet();
                }
    
                if (attempts.get() == 0) {
                    System.err.println("Failed to achieve quorum for operation ID: " + operationId); 
                }
             
            future.complete(false);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
                future.completeExceptionally(e);
                
            }
        }).start();
        return future;
    }


    private boolean waitForQuorumSync(String operationId) {
        CompletableFuture<Boolean> quorumFuture = waitForQuorum(operationId);
        try {
            return quorumFuture.get(); // Blocks until the quorum check completes
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    
    private void retrySyncProcess(String operationId) {
        int maxRetries = 5;
        int retryDelay = 5000;
        AtomicInteger retryCount = new AtomicInteger(0);    
        while (retryCount.get() < maxRetries) {
            try {
                System.out.println("Retrying sync process for operation ID: " + operationId + " (attempt " + (retryCount.get() + 1) + ")");

                boolean quorumAchieved = waitForQuorumSync(operationId);

                if (quorumAchieved) {
                    commitSyncProcess(operationId);
                    return; // Exit the retry loop after successful quorum
                } else {
                    retryCount.incrementAndGet();
                    System.err.println("Quorum not achieved. Retry attempt " + retryCount.get());
                }

                Thread.sleep(retryDelay);
                // waitForQuorum(operationId).thenAccept(success -> {
                //     if (success) {
                //         commitSyncProcess(operationId);
                //     } else if ( retryCount.incrementAndGet() == maxRetries) {
                //         System.err.println("Max retries reached for operation ID: " + operationId);
                //         System.err.println("Reverting changes....");
                //         revertChanges(temp);  
                //         System.err.println("Changes reverted....");
                //     } 
                // }).exceptionally(e -> {
                //     e.printStackTrace();
                //     return null;
                // });
                //retryCount.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.err.println("Max retries reached for operation ID: " + operationId);
        clearOperationsBatch();
        cancelDistributedOperation(operationId);
        documents.revertChanges();
        // revertChanges(new ArrayList<Document>(temp));
    
        
    }
    

                                                            /*
                                                            ██████  ██████  ███    ███ ███    ███ ██ ████████ 
                                                            ██      ██    ██ ████  ████ ████  ████ ██    ██    
                                                            ██      ██    ██ ██ ████ ██ ██ ████ ██ ██    ██    
                                                            ██      ██    ██ ██  ██  ██ ██  ██  ██ ██    ██    
                                                            ██████  ██████  ██      ██ ██      ██ ██    ██    
                                                                                                            
                                                            */


    public synchronized void commitSyncProcess(String operationId){
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

    


/*
███████ ██    ██ ██      ██          ███████ ██    ██ ███    ██  ██████ 
██      ██    ██ ██      ██          ██       ██  ██  ████   ██ ██      
█████   ██    ██ ██      ██          ███████   ████   ██ ██  ██ ██      
██      ██    ██ ██      ██               ██    ██    ██  ██ ██ ██      
██       ██████  ███████ ███████     ███████    ██    ██   ████  ██████ 
                                                                       
 */

    // Create updated DB in new Node
    public Message startFullSyncProcess(){
        String operationID = UniqueIdGenerator.generateOperationId(OPERATION.FULL_SYNC_ANS.hashCode() + Long.toString(System.currentTimeMillis()));; 

        // String payload = operationID + ";" + getNodeId() + ":" + this.gossipNode.getHeartbeatService().getUDPport() + ";";
        // for (Document doc: documentsList){
        //     payload = payload + String.join("$" , doc.toString());
        // }
        // System.out.println("documents sent to full sync: " + documentsList);
        // Start building the payload with operation ID and node details
        StringBuilder payloadBuilder = new StringBuilder(operationID)
                                                .append(";")
                                                .append(getNodeId())
                                                .append(":")
                                                .append(this.gossipNode.getHeartbeatService().getUDPport())
                                                .append(";");

        // Retrieve all documents from DocumentsDB and add to payload
        documents.getDocumentsMap().values().forEach(doc -> {
        payloadBuilder.append(doc.toString()).append("$");
        });
        // Remove trailing "$" if there are documents
        if (payloadBuilder.charAt(payloadBuilder.length() - 1) == '$') {
            payloadBuilder.deleteCharAt(payloadBuilder.length() - 1);
        }
        System.out.println("Documents sent to full sync: " + documents.getDocumentsMap().values());
        //distributedOperationsDescription.put(operationID, operationsBatch);
        distributedOperationsDesignation.put(operationID, "FULL_SYNC");

        Message fullSyncContent = new Message(OPERATION.FULL_SYNC_ANS, payloadBuilder.toString());
        return fullSyncContent;
    }
    protected synchronized void commitFullSync(String op){
        //System.out.println("Inside full sync commit");
        distributedOperations.put(op, "COMPLETED");
        //System.out.println(distributedOperations.toString());
        //distributedOperations.put(op, "WAITING");
    }
}