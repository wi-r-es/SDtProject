package Nodes;


import shared.Message;
import shared.MessageQueue;
import shared.OPERATION;
import utils.UniqueIdGenerator;
import utils.PrettyPrinter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


import java.util.stream.Collectors;

import Nodes.Raft.RaftNode;
import Resources.Document;
import Services.AckServiceServer;
import remote.messageQueueServer;

import java.rmi.RemoteException;

/**
 * The Node class represents a node in a distributed system.
 * It extends the Thread class to run as a separate thread.
 */
public class Node extends Thread {
    private final String nodeId;
    private final UUID UID;
    private final GossipNode gossipNode;  
    private ConcurrentHashMap<UUID, Integer> knownNodes = new ConcurrentHashMap<>();  // Known node IDs with their UDP ports
    private ConcurrentHashMap<UUID, String> knownNodesNames = new ConcurrentHashMap<>();  // Known node IDs with their name
    //private ArrayList<Document> documentsList = new ArrayList<>(); 
    private DocumentsDB documents = new DocumentsDB();
    //private ArrayList<Document> temp = null;
    private ArrayList<String> operationsBatch = new ArrayList<>(); // Operations processed from message Queue
    private boolean isLeader;
    private messageQueueServer messageQueue; 
    private AckServiceServer ackS = null; // not used
    private ConcurrentHashMap<UUID, String> documentChangesACKS = new ConcurrentHashMap<>();  // to save acks for operations of syncing before commmit
    private ConcurrentHashMap<String, String> distributedOperations = new ConcurrentHashMap<>();  // to save BIG SCALE operationsID WITH ITS STATUS
    private ConcurrentHashMap<String, ArrayList<String>> distributedOperationsDescription = new ConcurrentHashMap<>();  // List of the Operation ID action
    private ConcurrentHashMap<String, String> distributedOperationsDesignation = new ConcurrentHashMap<>();  // Mapping of the operation ID to its general action (sync, full sync)
    private int quorum;

    private volatile boolean running = true; // to check whether the node is running or not 
    
    /**
     * The run method is executed when the thread starts.
     * It performs the following tasks:
     * 1. If the node is a leader:
     *    - Starts the leader services (RMI and ACK services). // ACK server not used anymore, deprecated
     * 2. While the node is running:
     *    - Checks if the node is a leader.
     *    - If the node is a leader:
     *      - Checks if there are messages in the message queue.
     *      - If there are messages, processes and commits them.
     *      - Checks if there are any distributed operations in progress.
     *      - Prints the status of distributed operations.
     *      - If the current term of the Raft node reaches 100, stops the node. (to test the raft leader election after failure)
     *    - Prints the list of known nodes.
     *    - Prints the list of documents in the DocumentsDB.
     *    - Sleeps for 1 second.
     * 3. If an InterruptedException occurs:
     *    - Preserves the interrupt status.
     *    - Prints a message indicating that the worker thread was interrupted.
     *    - Breaks the loop.
     * 4. If any other exception occurs:
     *    - Prints the stack trace of the exception.
     */
    @Override
    public void run() {
        try {
            if(isLeader())
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
                        if (   ((RaftNode) (this)).getCurrentTerm()==100   ){
                            this.stopRunning();
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
    /**
     * Constructor for the Node class.
     *
     * @param nodeId The ID of the node.
     * @throws RemoteException If a remote exception occurs.
     * @return Node
     */
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
    /**
     * Constructor for the Node class with a leader flag.
     *
     * @param nodeId The ID of the node.
     * @param L      Indicates if the node is a leader.
     * @throws RemoteException If a remote exception occurs.
     * @return Node
     */
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

    /**
     * Checks if there are messages in the message queue to be processed.
     *
     * @return True if there are messages, false otherwise.
     * @throws RemoteException If a remote exception occurs.
     */
    protected synchronized  boolean checkQueue() throws RemoteException{
        if (messageQueue != null){return  messageQueue.checkQueue();}
        else return false;
    }
    /**
     * Stops the node from running. (By changing the running variable to false)
     */
    public void stopRunning() {
        System.out.println("im gonna stop myself");
        running = false; 
    }

    private void cleanupOnShutdown() {
        System.out.println("Node " + this.nodeId + " shutting down.");
    }

    //Getters
    public UUID getNodeId() {
        return UID;
    }
    public String getNodeName(){
        return nodeId;
    }

    public GossipNode getGossipNode() {
        return gossipNode;
    }

    public int getPeerPort(UUID peerId) {
        // System.out.println("getting peer port " +peerId + "for node: " + this.UID );
        // System.out.println(knownNodes.toString());
        // System.out.println("done getting peer port");
        return knownNodes.get(peerId); 
    }
    /**
     * Returns a list of known nodes.
     *
     * @return A list of Map.Entry objects containing the UUID and port of the known nodes.
     */
    public Set<Map.Entry<UUID, Integer>> getKnownNodes() {
        return knownNodes.entrySet();
    }

    /** 
     * Add known node to map (uuid - udp port number).
     */
    public void addKnownNode(UUID nodeId, int port){
        knownNodes.put(nodeId,  port);
    }
    /**
     * Add known node to map (uuid - node name).
     */
    public void addKnownNode(UUID nodeId, String name){
        knownNodesNames.putIfAbsent(nodeId,  name);
    }
    /**
     * Add ACKS for sync process.
     * 
     * @param nodeId ID of Node that sent the ACK.
     * @param syncOP Operation Id.
    */
    public void addACK(UUID nodeId, String syncOP){
        documentChangesACKS.putIfAbsent(nodeId, syncOP);
    }
    /** 
     * Start Leader Services.
     */
    protected void startLeaderServices() throws RemoteException {
        startRMIService();       
        //startACKService();    
    }
    /** 
     * Start RMI Service.
     */
    public void startRMIService() throws RemoteException {
        try {
            messageQueue = new messageQueueServer(nodeId, 2323);
            messageQueue.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /** 
     * Set the flag isLeader to true for when the node becomes a leader. 
     */
    protected void becomeLeader(){
        this.isLeader=true;
    }

    /** 
     * Start ACK Service.
     */
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
    /** 
     * Returns a copy (for safety reasons) of the current known nodesList.
     */
    public List<Map.Entry<UUID, Integer>> getNodesList() {
        List<Map.Entry<UUID, Integer>> nodesList = new ArrayList<>(knownNodes.entrySet());
        return nodesList; 
    }
    /** 
     * Verify if the node is the current Leader.
     */
    public boolean isLeader(){
        return isLeader;
    }

    /**
     * Function to printify the list of known nodes in a neat way .
     */
    protected synchronized void printKnownNodes(){
        List<Map.Entry<UUID, Integer>> knownNodes = getNodesList();
        String[] headers = {"UUID", "Value"};
        List<String[]> rows = knownNodes.stream()
                .map(entry -> new String[]{entry.getKey().toString(), entry.getValue().toString()})
                .collect(Collectors.toList());

        // Print the table
        PrettyPrinter.printTable(headers, rows);
    }


    /**
     *  Getter for the documents instance of node.
     */
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
    /** 
     * Adds processed operation to batch.
     */
    public synchronized void addOperation(String op){
        operationsBatch.add(op);
    }

    /** 
     * Helper function to process Message from Message Queue. 
     * @param msg The Message to be processed.
     * */
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

    /** 
     * Helper function to process operation from Message Queue. 
     * @param op Operation regarding the document.
     * @param document Document related to the operation to be processed.
     */
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
                       addOperation("UPDATE;" + document.toString()); // uses the same function has above, but logs the orinal operation from the message queue accordingly.
           
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
    /** 
     * Overloading method for different class type for the first parameter.
     * Helper function to process operation from Message Queue .
     * @param op Operation regarding the document.
     * @param document Document related to the operation to be processed.
     */
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



    
    // Helper function to debug state of documents.
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
    /**
     * Adds the batch of operations through its operation generated ID to the map and set it to WAITING (for ACKS from other nodes).
     * @param op The Operartion ID regarding the batch operation to be distributed across the cluster.
     */
    private synchronized void addDistributedOperation(String op){
        distributedOperations.putIfAbsent(op, "WAITING");
        //distributedOperations.put(op, "WAITING");
    }
    /**
     * Commits distributed operation .
     * @param op The Operartion ID regarding the batch operation distributed across the cluster.
     */
    private synchronized void commitDistributedOperation(String op){
        distributedOperations.replace(op, "COMMITED");
        //distributedOperations.put(op, "WAITING");
    }
    /**
     * Cancels distributed operation .
     * @param op The Operartion ID regarding the batch operation distributed across the cluster.
     */
    private synchronized void cancelDistributedOperation(String op){
        distributedOperations.replace(op, "CANCELED");
        //Creates the message to cancel the previous sync request
        Message cancelSyncMessage = new Message(
                OPERATION.REVERT,     
                ";" +getNodeId() + ":" +this.gossipNode.getHeartbeatService().getUDPport() +";" 
            );
            gossipNode.getHeartbeatService().broadcast(cancelSyncMessage, true);
        System.out.println("Operation canceled");
        //distributedOperations.put(op, "WAITING");
    }
    /**
     * Checks whether the distributed operation was commited or not.
     * @param op The Operation ID.
     * @return true if operation was commited( as "FINISHED") false otherwise.
     */
    private synchronized boolean isCommited(String op){
        String state = distributedOperations.get(op);
        return state == "FINISHED" ? true : false; 
    }
    /**
     * Checks whether the distributed operation was CANCELLE.
     * @param op The Operation ID.
     * @return true if operation was cancelled( as "CANCELED") false otherwise.
     */
    private synchronized boolean isCancelled(String op){
        String state = distributedOperations.get(op);
        return state == "CANCELED" ? true : false; 
    }
    /**
    * Processes and commits the operations in the message queue.
    * 1. Locks the documents to synchronize access.
    * 2. Creates a temporary map of the documents.
    * 3. Processes messages from the message queue until it is empty:
    *    - Dequeues a message from the queue.
    *    - Processes the message based on its operation type (CREATE, UPDATE, DELETE).
    * 4. Starts the sync process and retrieves the operation ID.
    * 5. If the operation is committed:
    *    - Commits the changes to the documents.
    *    - Prints the state of the documents after committing.
    * 6. If the operation is canceled:
    *    - Prints an error message indicating that quorum was not achieved.
    *    - Reverts the changes to the documents.
    * 7. Unlocks the documents.
    * 8. If a RemoteException occurs, prints the stack trace.
    */
    protected void processAndCommit() {
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
    /**
     * Starts the sync process for distributed operations.
     * 1. Updates the quorum value based on the number of known nodes.
     * 2. Generates a unique operation ID using the hash code of the operations batch.
     * 3. Adds the operation ID to the distributed operations map with a status of "WAITING".
     * 4. Stores the operations batch in the distributed operations description map.
     * 5. Sets the designation of the operation as "SYNC".
     * 6. Filters out duplicate operations from the operations batch.
     * 7. Creates a sync message with the operation ID, node ID, UDP port, and joined unique operations.
     * 8. Broadcasts the sync message to all known nodes.
     * 9. Waits for a quorum of acknowledgments using the `waitForQuorum` method.
     * 10. If the quorum is achieved, commits the sync process.
     * 11. If the quorum is not achieved, retries the sync process.
     * 12. If an exception occurs during the sync process, prints the stack trace.
     * 13. Returns the operation ID.
     *
     * @return The operation ID of the sync process.
     */
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

    /**
     * Updates the quorum value based on the number of known nodes.
     * The quorum is calculated as (number of nodes / 2) + 1.
     */
    private synchronized void updateQuorum(){
        long N = knownNodes.mappingCount();
        this.quorum= ((int)N / 2) + 1;
    }
    /** 
     * Getter for the previously calculated minimum quorum size. 
     */
    private int getQuorum(){
       return this.quorum;
    }           
    /**
     * Waits for a quorum of acknowledgments for a given operation ID.
     * 1. Creates a CompletableFuture to represent the quorum check.
     * 2. Starts a new thread to perform the quorum check.
     * 3. Initializes the maximum number of attempts and the delay between attempts.
     * 4. While there are remaining attempts:
     *    - Counts the number of acknowledgments received for the operation ID.
     *    - If the count reaches the quorum, completes the future with true and returns.
     *    - Prints debug information about missing acknowledgments.
     *    - Sleeps for the specified delay.
     *    - Decrements the number of attempts.
     * 5. If the maximum number of attempts is reached without achieving quorum:
     *    - Prints an error message.
     *    - Completes the future with false.
     * 6. If an InterruptedException occurs:
     *    - Preserves the interrupt status.
     *    - Prints the stack trace.
     *    - Completes the future exceptionally with the exception.
     *
     * @param operationId The operation ID.
     * @return A CompletableFuture that completes with a boolean indicating if the quorum was achieved.
     */                                                     
    @SuppressWarnings("unused")
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

    /**
     * Waits for a quorum of acknowledgments for a given operation ID synchronously.
     * Calls `waitForQuorum` and blocks until the quorum check completes.
     *
     * @param operationId The operation ID.
     * @return True if the quorum was achieved, false otherwise.
     */
    private boolean waitForQuorumSync(String operationId) {
        CompletableFuture<Boolean> quorumFuture = waitForQuorum(operationId);
        try {
            return quorumFuture.get(); // Blocks until the quorum check completes
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    /**
     * Retries the sync process for a given operation ID.
     * 1. Initializes the maximum number of retries and the delay between retries.
     * 2. While the retry count is less than the maximum retries:
     *    - Prints a retry attempt message.
     *    - Calls `waitForQuorumSync` to wait for quorum synchronously.
     *    - If quorum is achieved, commits the sync process and returns.
     *    - Increments the retry count and prints an error message.
     *    - Sleeps for the specified retry delay.
     * 3. If the maximum retries are reached without achieving quorum:
     *    - Prints an error message.
     *    - Clears the operations batch.
     *    - Cancels the distributed operation.
     *    - Reverts the changes to the documents.
     *
     * @param operationId The operation ID.
     */
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
    }
    

                                                            /*
                                                            ██████  ██████  ███    ███ ███    ███ ██ ████████ 
                                                            ██      ██    ██ ████  ████ ████  ████ ██    ██    
                                                            ██      ██    ██ ██ ████ ██ ██ ████ ██ ██    ██    
                                                            ██      ██    ██ ██  ██  ██ ██  ██  ██ ██    ██    
                                                            ██████  ██████  ██      ██ ██      ██ ██    ██    
                                                                                                            
                                                            */

    /**
    * Commits the sync process for a given operation ID.
    * Sends a commit message, clears the operations batch, and marks the distributed operation as committed.
    *
    * @param operationId The operation ID.
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
    /**
    * Sends a commit message for a given operation ID.
    * Creates a commit message and broadcasts it to all nodes without requiring acknowledgment.
    *
    * @param operationID The operation ID.
    */
    private void sendCommitMessage(String operationID){
        Message syncMessage = new Message(
                OPERATION.COMMIT,    
                operationID
            );
            gossipNode.getHeartbeatService().broadcast(syncMessage, false);
    }
    /**
    * Clears the operations batch by removing all elements from the list.
    */
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

    /**
     * Starts the full sync process to create an updated DB in a new node.
     * Generates an operation ID, builds the payload with node details and all documents, 
     * and returns a message containing the full sync content.
     *
     * @return The message containing the full sync content.
     */
    public Message startFullSyncProcess(){
        String operationID = UniqueIdGenerator.generateOperationId(OPERATION.FULL_SYNC_ANS.hashCode() + Long.toString(System.currentTimeMillis()));; 
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
    /**
     * Commits Full Sync operation .
     * @param op The Operartion ID regarding the batch operation.
     */
    protected synchronized void commitFullSync(String op){
        //System.out.println("Inside full sync commit");
        distributedOperations.put(op, "COMPLETED");
        //System.out.println(distributedOperations.toString());
        //distributedOperations.put(op, "WAITING");
    }







    
}