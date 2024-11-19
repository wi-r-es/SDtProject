package Services;
import Nodes.*;
import Resources.Document;
import shared.Message;
import shared.OPERATION;
import utils.CompressionUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HeartbeatService extends Thread {

    private final GossipNode gossipNode;
    private final Map<UUID, AtomicInteger> heartbeatCounters;  // heartbeat counter for each node
    private final Map<String, Long> lastReceivedHeartbeats;  // last received heartbeat timestamps
    private final ScheduledExecutorService scheduler; // for running heartbeats regularly [and fail detection in the future]
    private DatagramSocket socket;
    private int udpPort;
    private static final int HEARTBEAT_INTERVAL = 5000;  // Interval in milliseconds for sending heartbeats
    private static final int FAILURE_TIMEOUT = 10000;  // Timeout to detect failure (ms)

    private static final int NODE_PORT_BASE = 9678;  // base port for UDP communication

    private static final int PORT = 9876;  // UDP communication multicast
    private static final String MULTICAST_GROUP = "230.0.0.0";  

    //for ack syncs
    private final int TCP_PORT = 9090;

    public HeartbeatService(GossipNode node) {
        this.gossipNode = node;
        this.heartbeatCounters = new ConcurrentHashMap<>();
        this.lastReceivedHeartbeats = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(2); //one for heartbeat one for fail detection

        try {
            //this.socket = new DatagramSocket(NODE_PORT_BASE);
            // this.socket.setSoTimeout(HEARTBEAT_INTERVAL);
            
            //GOSSIP PROTO
            this.udpPort = NODE_PORT_BASE + Math.abs(gossipNode.getNodeId().hashCode()) % 1000;
            this.socket = new DatagramSocket(udpPort);  // Unique port 
           
        } catch (SocketException e) {
            e.printStackTrace();
        }

        heartbeatCounters.put(gossipNode.getNodeId(), new AtomicInteger(0));  // Initializes heartbeat counter
    }

    @Override
    public String toString(){
        return "Node{id='" + gossipNode.getNodeName()  +  "', port='" + this.getUDPport() + "'}";
    }

    public int getUDPport(){
        return this.udpPort;
    }

    // Start heartbeat incrementing and failure detection tasks using scheduler
    @Override
    public void run() {        
        // broadcast way
        scheduler.scheduleAtFixedRate(this::incrementAndBroadcastHeartbeat, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);

        // Detection failure
        //scheduler.scheduleAtFixedRate(this::detectFailures, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);

        // Start a separate thread for continuously receiving heartbeats
        new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                //receiveHeartbeatsGossip();
                //receiveHeartbeats();
                receiveMulticast();
            }
        }).start();
        new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                receiveMessage();
            }
        }).start();

    }
    
    //Broadcast implementation
    private void incrementAndBroadcastHeartbeat() {
        //incrementHeartbeat();
        if(gossipNode.isLeader())
        {
           // System.out.println(gossipNode.getNodeId());
            //System.out.println(gossipNode.isLeader());
            Message hb_message = Message.heartbeatMessage("Heartbeat:" + gossipNode.getNodeId() + ":" + this.udpPort + ":" + MULTICAST_GROUP + ":" + incrementHeartbeat()); 
            // Equivalent message to Sending heartbeat from UUID to 230.0.0.0 with counter 1
            this.broadcast(hb_message, false);
            //this.broadcastHeartbeat();
        }

    }

    private int getHeartbeatCounter() {
        return heartbeatCounters.get(gossipNode.getNodeId()).get();
    }
    private int incrementHeartbeat() {
        return heartbeatCounters.get(gossipNode.getNodeId()).incrementAndGet();
    }



    //BROADCAST
    public void broadcast(Message message, boolean compress) {
        try (MulticastSocket multicastSocket = new MulticastSocket()) {

            byte[] serializedData = serialize(message);
    

            byte[] finalData;
            if (compress) {
                byte[] compressedData = CompressionUtils.compress(serializedData);
                finalData = addHeader("COMP", compressedData);
            } else {
                finalData = addHeader("UNCO", serializedData);
            }
    

            InetAddress group = InetAddress.getByName(MULTICAST_GROUP);
            DatagramPacket packet = new DatagramPacket(finalData, finalData.length, group, PORT);
            multicastSocket.send(packet);
    
            System.out.println("Broadcasting message: " + message.getOperation() + " with compression=" + compress + " content=[" +  "]");
        } catch (IOException e) {
            System.err.println("Error broadcasting message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void respondeToHeartbeat(UUID targetNodeId, int target_port) {
        try {
            Message msg = Message.replyHeartbeatMessage("ACK_Heartbeat:" + gossipNode.getNodeId() + ":" + this.udpPort + ":"  + heartbeatCounters.get(gossipNode.getNodeId()).getAndIncrement() + ":" + System.currentTimeMillis());
            byte[] serializedData = serialize(msg);
            byte[] finalData = addHeader("UNCO", serializedData);
                
            InetAddress targetAddress = InetAddress.getByName(getNodeIPAddress(targetNodeId));

            DatagramPacket packet = new DatagramPacket(finalData, finalData.length, targetAddress, target_port);
            socket.send(packet);

            System.out.println("ACK PACKET SENT FROM " + gossipNode.getNodeId() + " to " + targetNodeId + " with counter " + getHeartbeatCounter());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void replyToHeartbeat(Message message){
        Object obj = message.getPayload();
        String content = (String) obj;
        System.out.println("\tCotent inside reply to heartbeat->  " + content+"\n\n");
        String[] parts = content.split(":");
        String senderNodeId = parts[1];
        int target_port = Integer.parseInt(parts[2]);
        if(gossipNode.getNodeId().equals(UUID.fromString(senderNodeId))) { return;}
        int heartbeatCounter = Integer.parseInt(parts[4]);

        // Update local heartbeat data

        //computeIfAbsente looks for an entry in "heartbeatCounters" with the key senderNodeId.
        // if key not present, creates a nwe entry with amoticInteger initialization with initialValue = 0
        // updateandGet will atomically updated the value retrieved from before 
        heartbeatCounters.computeIfAbsent(UUID.fromString(senderNodeId), k -> new AtomicInteger(0))
            .updateAndGet(current -> Math.max(current, heartbeatCounter));
        lastReceivedHeartbeats.put(senderNodeId, System.currentTimeMillis());
        
        respondeToHeartbeat(UUID.fromString(senderNodeId), target_port );        
    }

    private void addKnownNode(Message message){
        Object obj = message.getPayload();
        String content = (String) obj;
        System.out.println("\tCotent inside add to knownNodes->  " + content+"\n\n");
        String[] parts = content.split(":");
        String senderNodeId = parts[1];
        int port = Integer.parseInt(parts[2]);

        gossipNode.addKnownNode(UUID.fromString(senderNodeId), port);
    }

    /*
  _____               _             __  __                                       __  __       _ _   _     ___    _       _ 
 |  __ \             (_)           |  \/  |                                     |  \/  |     | | | (_)   / / |  | |     (_)
 | |__) |___  ___ ___ ___   _____  | \  / | ___  ___ ___  __ _  __ _  ___  ___  | \  / |_   _| | |_ _   / /| |  | |_ __  _ 
 |  _  // _ \/ __/ _ \ \ \ / / _ \ | |\/| |/ _ \/ __/ __|/ _` |/ _` |/ _ \/ __| | |\/| | | | | | __| | / / | |  | | '_ \| |
 | | \ \  __/ (_|  __/ |\ V /  __/ | |  | |  __/\__ \__ \ (_| | (_| |  __/\__ \ | |  | | |_| | | |_| |/ /  | |__| | | | | |
 |_|  \_\___|\___\___|_| \_/ \___| |_|  |_|\___||___/___/\__,_|\__, |\___||___/ |_|  |_|\__,_|_|\__|_/_/    \____/|_| |_|_|
                                                                __/ |                                                      
                                                               |___/                                                       
     */
    

    private void receiveMulticast() { 
        try (MulticastSocket multicastSocket = new MulticastSocket(PORT)) {
            InetAddress group = InetAddress.getByName(MULTICAST_GROUP);
            multicastSocket.joinGroup(group);
            System.out.println("Listening on multicast group: " + MULTICAST_GROUP);
    
            byte[] buffer = new byte[2048]; 
    
            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                multicastSocket.receive(packet);
    
                // Extract the header (first 4 bytes) and payload
                String header = new String(packet.getData(), 0, 4);
                byte[] payload = Arrays.copyOfRange(packet.getData(), 4, packet.getLength());
    
                if ("COMP".equals(header)) {
                    System.out.println("Received compressed message");
                    // Decompress and deserialize
                    byte[] decompressedData = CompressionUtils.decompress(payload);
                    Message message = (Message) deserialize(decompressedData);

                    
                    OPERATION op = message.getOperation();
                    Object obj = message.getPayload();
                    //Document doc = (Document) obj;
                        
                    switch (op) {
                        case SYNC: //for syncing purposes
                            if(this.gossipNode.isLeader()){ break;}
                            System.out.println("IN SYNC BROADCAST RECEIVE");
                            System.out.print("\t");
                            System.out.println(obj);
                            processSync(obj);
                            //sendSyncAck(UUID targetNodeId, int target_port)
                            break;
                        default:
                            System.err.println("This operation is not supported in this part of the code, BIG BUG" + op);
                    }


                    System.out.println("Received compressed message");
                } else if ("UNCO".equals(header)) {

                    Message message = (Message) deserialize(payload);
                    OPERATION op = message.getOperation();
                    switch (op) {
                        case HEARTBEAT: // reply to hearbeats 
                        //System.out.println("\n\tIM HERE IN CASE HEARTBEAT\n");
                            replyToHeartbeat(message);

                            break;
                        case COMMIT: // for commit purposes
                            System.out.println("\n\n\t COMMITED -> " + message + "\n\n");
                            break;
                        case DISCOVERY: // for NEW NODE 
                            if (this.gossipNode.isLeader()){
                                processDiscoveryRequest(message);
                            }
                            break;
                        default:
                            System.err.println("This operation is not supported in this part of the code, BIG BUG" + op);
                    }

                    System.out.println(this.gossipNode.getNodeName()+"Received uncompressed message: " + message);
                } else {
                    System.out.println("Unknown message type: " + header);
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Error receiving message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void receiveMessage() {
        byte[] buffer = new byte[2048]; 
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        
        try{
            socket.setSoTimeout(5000);
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    socket.receive(packet); 
                    System.out.println("Message received in receiveMessage on port: " + packet.getPort());
                    System.out.println("Packet content: " + new String(packet.getData(), 0, packet.getLength()));
                    // Extract the header (first 4 bytes) and payload
                    String header = new String(packet.getData(), 0, 4);
                    byte[] payload = Arrays.copyOfRange(packet.getData(), 4, packet.getLength());


                    if ("COMP".equals(header)) {
                        System.out.println("Received compressed message");
                        // Decompress and deserialize
                        byte[] decompressedData = CompressionUtils.decompress(payload);
                        Message message = (Message) deserialize(decompressedData);

                        
                        OPERATION op = message.getOperation();
                        Object obj = message.getPayload();
                        //Document doc = (Document) obj;
                            
                        switch (op) {
                            case FULL_SYNC: //for full sync for new node
                            if (this.gossipNode.isLeader()){
                                // Message msg = Message.FullSyncMessage("FULL_SYNC:" + gossipNode.getNodeId() + ":" + sync_port + ":"  + System.currentTimeMillis());
                                String receivedMsg = (String)obj;
                                // Split string to get usefull data
                                String[] parts = receivedMsg.split(":");
                                String nodeId = parts[1];
                                String port = parts[2];
                                // Get the documents list
                                Message msg = this.gossipNode.startFullSyncProcess(); 
                                // Send the document list to new node
                                leaderRespondToFullSync(msg, UUID.fromString(nodeId), Integer.parseInt(port));}
                                break;

                            default:
                                System.err.println("This operation is not supported in this part of the code, BIG BUG" + op);
                        }


                        System.out.println("Received compressed message");
                        
                    } else if ("UNCO".equals(header)) {
                        
                        Message message = (Message) deserialize(payload);
                        OPERATION op = message.getOperation();
                        switch (op) {
                            case HEARTBEAT_ACK: // reply to hearbeats 
                                System.out.println("\n\tIM HERE IN CASE HEARTBEAT_ACK\n");
                                if(this.gossipNode.isLeader()){
                                    addKnownNode(message);
                                }
                                break;
                            
                            case ACK: 
                                if (this.gossipNode.isLeader()){
                                    System.out.println("\n\n\t ACK RECEIVED FOR OPERATION: " + message);
                                    Object obj = message.getPayload();
                                    processACK(obj);
                                    
                                }
                                break;
                            default:
                                System.err.println("This operation is not supported in this part of the code, BIG BUG" + op);
                        }

                        System.out.println(this.gossipNode.getNodeName()+"Received uncompressed message: " + message);
                    } else {
                        System.out.println("Unknown message type: " + header);
                    }
                } catch (SocketTimeoutException e) {
                    // Timeout occurred; continue waiting for messages
                    System.out.println("Socket timeout; no message received.");
                }
            }
        }
         catch (IOException | ClassNotFoundException e) {
            System.err.println("Error receiving message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    

/*
   _____                        _____ _  __
  / ____|                 /\   / ____| |/ /
 | (___  _   _ _ __      /  \ | |    | ' / 
  \___ \| | | | '_ \    / /\ \| |    |  <  
  ____) | |_| | | | |  / ____ \ |____| . \ 
 |_____/ \__, |_| |_| /_/    \_\_____|_|\_\
          __/ |                            
         |___/                             
 */

    public String processBatch(String batch) {
        System.out.println("\n\n\t\tINSIDE PROCESS BATCH\n\n\n");
        // Split the batch into operation ID and operations
        String[] parts = batch.split(";", 3); // split the message into the each section of it
        String operationId = parts[0];
        String leaderInfo = parts[1];
        String operations = parts[2];
        System.out.println("\t\t" + leaderInfo + "\n\t" + operations );
        System.out.println("Processing batch with Operation ID: " + operationId);
        
        // Split individual operations
        String[] operationArray = operations.split("$");
        boolean result = false;

        
        try{
            for (String operation : operationArray) {
                System.out.println("\nProcessing operation: " + operation);

                Pattern pattern = Pattern.compile("id='(.*?)', content='(.*?)', version='(\\d+)'\\}");
                Matcher matcher = pattern.matcher(operation);

                if (matcher.find()) {
                    String id = matcher.group(1);
                    String content = matcher.group(2);
                    int version = Integer.parseInt(matcher.group(3));

                    // System.out.println("Document Details:");
                    // System.out.println("  ID: " + id);
                    // System.out.println("  Content: " + content);
                    // System.out.println("  Version: " + version);

                    // Process the operation 
                    if (operation.startsWith("CREATE")) {
                        result = handleCreate(new Document(content, UUID.fromString(id), version));
                    } else if (operation.startsWith("UPDATE")) {
                        result =  handleUpdate(new Document(content, UUID.fromString(id), version));
                    } else if (operation.startsWith("DELETE")) {
                        result =  handleDelete(new Document(content, UUID.fromString(id), version));
                    }
                } else {
                    System.err.println("Invalid document format in operation: " + operation);
                }
            }
            return result ? (operationId +":" +leaderInfo ): null;
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
        
    }


    private boolean handleCreate(Document document) {
        System.out.println("Handling CREATE for: " + document);
        return gossipNode.addDocument(document);  
    }

    private boolean handleUpdate(Document document) {
        System.out.println("Handling UPDATE for: " + document);
        int res = gossipNode.searchDocument(document);
        if(res == 1){
            return gossipNode.updateDocument(document);
        }
        else if (res==-1){
           return gossipNode.addDocument(document);
        }
        return false;
        
    }

    private boolean handleDelete(Document document) {
        System.out.println("Handling DELETE for: " + document);
        return gossipNode.removeDocument(document);
    }


    private boolean processSync(Object obj){
        String infoLeader = processBatch((String)obj);
        System.out.println(infoLeader);
        if(infoLeader != null){
            String[] parts = infoLeader.split(":");
            String opID = parts[0]; //OP id
            String LID = parts[1]; //Leader id
            String port = parts[2]; //Leader port
            sendSyncAck(UUID.fromString(LID), Integer.parseInt(port), opID);
        }
        return false;
    }

    // private void sendSyncAckTCPServer(Message msg) {
    //     try (
    //         Socket tcpS = new Socket("localhost", TCP_PORT);
    //         ObjectOutputStream out = new ObjectOutputStream(tcpS.getOutputStream());
    //         ObjectInputStream in = new ObjectInputStream(tcpS.getInputStream())
    //     ) {
    //         // Send the custom Message object
    //         out.writeObject(msg);
    //         // Receive the response from the server
    //         Message response = (Message) in.readObject();
    //         System.out.println("Server Response: " + response);

    //     } catch (IOException | ClassNotFoundException e) {
    //         e.printStackTrace();
    //     }
    // }
    private void sendSyncAck(UUID targetNodeId, int target_port, String operationID) {
        try {
            Message msg = Message.replySyncMessage("ACK:" + gossipNode.getNodeId() + ":" + operationID + ":" + 
                                                        heartbeatCounters.get(gossipNode.getNodeId()).getAndIncrement() + ":" + System.currentTimeMillis());
            byte[] serializedData = serialize(msg);
            byte[] finalData = addHeader("UNCO", serializedData);
                
            InetAddress targetAddress = InetAddress.getByName(getNodeIPAddress(targetNodeId));

            DatagramPacket packet = new DatagramPacket(finalData, finalData.length, targetAddress, target_port);
            socket.send(packet);

            System.out.println("ACK PACKET SENT FROM " + gossipNode.getNodeId() + " to " + targetNodeId + " with counter " + getHeartbeatCounter());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
//
    private void processACK(Object obj){
        String content = (String) obj;
        String[] parts = content.split(":");
        String senderNodeId = parts[1];
        String opID = parts[2];
        
        this.gossipNode.addACK(UUID.fromString(senderNodeId), opID);
        

    }




















    
    

    /*
    
                _____ _____  _____  _____ ______      ________ _______     __
                |  __ \_   _|/ ____|/ ____/ __ \ \    / /  ____|  __ \ \   / /
                | |  | || | | (___ | |   | |  | \ \  / /| |__  | |__) \ \_/ / 
                | |  | || |  \___ \| |   | |  | |\ \/ / |  __| |  _  / \   /  
                | |__| || |_ ____) | |___| |__| | \  /  | |____| | \ \  | |   
                |_____/_____|_____/ \_____\____/   \/   |______|_|  \_\ |_|   
                                                                            
                                                               
    */
    private void processDiscoveryRequest(Message message){
        Object obj = message.getPayload();
        String content = (String) obj;
        System.out.println("\tCotent inside add to knownNodes->  " + content+"\n\n");
        String[] parts = content.split(":"); //String PAYLOAD = "WHOS_THE_LEADER:" + nodeId + ":" + port + ":" + System.currentTimeMillis();
        String senderNodeId = parts[1];
        int port = Integer.parseInt(parts[2]);

        sendACKDiscovery(UUID.fromString(senderNodeId),port );

        gossipNode.addKnownNode(UUID.fromString(senderNodeId), port);
    }
    private void sendACKDiscovery(UUID targetNodeId, int target_port) {
        try {
            Message msg = Message.replyDiscoveryMessage("DISCOVERY_ACK:" + gossipNode.getNodeId() + ":" + this.udpPort + ":" + System.currentTimeMillis());
            byte[] serializedData = serialize(msg);
            byte[] finalData = addHeader("UNCO", serializedData);
                
            InetAddress targetAddress = InetAddress.getByName(getNodeIPAddress(targetNodeId));

            DatagramPacket packet = new DatagramPacket(finalData, finalData.length, targetAddress, target_port);
            socket.send(packet);

            System.out.println("DISCOVERY_ACK PACKET SENT FROM " + gossipNode.getNodeId() + " to " + targetNodeId + " to port " + target_port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String broadcastDiscovery(int port, Message msg){
        try(DatagramSocket syncSocket = new DatagramSocket(port)){
            syncSocket.setSoTimeout(5000);
            //2nd sent the multicast
            broadcast(msg, false);
            System.out.println("Waiting for response to discovery message...");

            byte[] buffer = new byte[2048]; 
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                // Wait for a response
                syncSocket.receive(packet);
    
                // Deserialize the response
                byte[] data = Arrays.copyOfRange(packet.getData(), 4, packet.getLength());
                Message responseMessage = (Message) deserialize(data);
    
                // Process the response
                if (responseMessage.getOperation() == OPERATION.DISCOVERY_ACK) {
                    System.out.println("Received sync response: " + responseMessage);
                    // Process sync response (e.g., synchronize documents or state)
                    String res=  handleFullSyncResponse(responseMessage);
                    return res;
                } else {
                    System.err.println("Unexpected operation: " + responseMessage.getOperation());
                }
            } catch (SocketTimeoutException e) {
                System.err.println("Timeout waiting for sync response.");
            } catch (ClassNotFoundException e) {
                            e.printStackTrace();
            }
            
        }catch(IOException e ){
            e.printStackTrace();
        }
        return null;
    }

    /*
        _______ _    _ _      _         _______     ___   _  _____
        |  ____| |  | | |    | |       / ____\ \   / / \ | |/ ____|
        | |__  | |  | | |    | |      | (___  \ \_/ /|  \| | |     
        |  __| | |  | | |    | |       \___ \  \   / | . ` | |     
        | |    | |__| | |____| |____   ____) |  | |  | |\  | |____ 
        |_|     \____/|______|______| |_____/   |_|  |_| \_|\_____|

     */

    
    private String handleFullSyncResponse(Message response){
        Object obj = response.getPayload();
        String content = (String) obj;
        System.out.println("\tContent inside add to knownNodes->  " + content+"\n\n");
        String[] parts = content.split(":"); //("DISCOVERY_ACK:" + gossipNode.getNodeId() + ":" + this.udpPort + ":" + System.currentTimeMillis());
        String LeaderNodeId = parts[1];
        String Leader_UDP_port = parts[2];
        int port = Integer.parseInt(Leader_UDP_port);
        gossipNode.addKnownNode(UUID.fromString(LeaderNodeId),port );

        return LeaderNodeId + ":" + Leader_UDP_port;        
    }

    private void fullSyncRequest(UUID leaderNodeId, int leader_port, int sync_port) {
        try {
            Message msg = Message.FullSyncMessage("FULL_SYNC:" + gossipNode.getNodeId() + ":" + sync_port + ":"  + System.currentTimeMillis());
            byte[] serializedData = serialize(msg);
            byte[] compressedData = CompressionUtils.compress(serializedData);
            byte[] finalData = addHeader("COMP", compressedData);
                
            InetAddress targetAddress = InetAddress.getByName(getNodeIPAddress(leaderNodeId));

            DatagramPacket packet = new DatagramPacket(finalData, finalData.length, targetAddress, leader_port);
            socket.send(packet);

            System.out.println("FULL_SYNC SENT FROM " + gossipNode.getNodeId() + " to " + leader_port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void fullSyncInnit(UUID leaderID, int leader_port, int sync_port ){
        try(DatagramSocket syncSocket = new DatagramSocket(sync_port)){
            syncSocket.setSoTimeout(50000);
            //2nd sent the request
            fullSyncRequest(leaderID, leader_port, sync_port);
            // Await for updated DB
            Message data = waitFullSync(syncSocket);
            System.out.println(data);
            Object obj = data.getPayload();

            System.out.println("\n\n\n\t\t\tFULL SYNC RECEIVED\n\n\n");

            System.out.println("OBJECT: " + obj);
            String list = (String) obj; 
            processDocumentList(list);
        }catch(SocketException e){
            System.out.println("SOCKET EXCEPTION");
        }catch(IOException e ){
            e.printStackTrace();
        }
    }

    private Message waitFullSync(DatagramSocket socket){
        byte[] buf = new byte[2048];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        int attempts = 100;
        try{
            while (attempts > 0) {
                socket.receive(packet);
                //Extract header's first 4 bytes
                String header = new String(packet.getData(), 0, 4);
                // Get compressed list of new docs
                byte[] payload = Arrays.copyOfRange(packet.getData(), 4, packet.getLength());
                
                byte[] decompressedData = CompressionUtils.decompress(payload);
                Message msg = (Message)deserialize(decompressedData);
                System.out.println("MESSAGE: " + msg);
                OPERATION op = msg.getOperation();

                if(op == OPERATION.FULL_SYNC_ANS){
                    return msg;
                }
                System.err.println("Operation not premited");
                attempts--;
            }
        }catch(Exception e){
            System.out.println("Error: " + e);
        }

        return null;
    }

    private void leaderRespondToFullSync(Message msg, UUID targetNodeId, int target_port) {
        try {            
            byte[] serializedData = serialize(msg);
            byte[] compressedData = CompressionUtils.compress(serializedData);
            byte[] finalData = addHeader("COMP", compressedData);
                
            InetAddress targetAddress = InetAddress.getByName(getNodeIPAddress(targetNodeId));

            DatagramPacket packet = new DatagramPacket(finalData, finalData.length, targetAddress, target_port);
            socket.send(packet);

            System.out.println("FULL_SYNC PACKET SENT FROM " + gossipNode.getNodeId() + " to " + targetNodeId + " with counter " + getHeartbeatCounter());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String processDocumentList(String batch) {
        System.out.println("\n\n\t\tINSIDE PROCESS FULL SYNC\n\n\n");
        // Split the batch into operation ID and operations
        String[] parts = batch.split(";", 3); // split the message into the each section of it
        String operationId = parts[0];
        String leaderInfo = parts[1];
        String documents = parts[2];
        System.out.println("\t\t" + leaderInfo + "\n\t" + documents );
        System.out.println("Processing batch with Operation ID: " + operationId);
        
        // Split individual operations
        String[] docs = documents.split("$");
        boolean result = false;

        
        try{
            for (String doc : docs) {
                System.out.println("\nProcessing documents list: " + doc);

                Pattern pattern = Pattern.compile("id='(.*?)', content='(.*?)', version='(\\d+)'\\}");
                Matcher matcher = pattern.matcher(doc);

                if (matcher.find()) {
                    String id = matcher.group(1);
                    String content = matcher.group(2);
                    int version = Integer.parseInt(matcher.group(3));

                    System.out.println("Document Details:");
                    System.out.println("  ID: " + id);
                    System.out.println("  Content: " + content);
                    System.out.println("  Version: " + version);

                    Document doc_aux = new Document(content, UUID.fromString(id), version);

                    if(gossipNode.documentListEmpty()){
                        gossipNode.addDocument(doc_aux);
                    }else if(gossipNode.searchDocument(doc_aux) == -1){
                        gossipNode.addDocument(doc_aux); 
                    }else if(gossipNode.searchDocument(doc_aux) != -1){
                        gossipNode.updateDocument(doc_aux);
                    }
                    
                } else {
                    System.err.println("Invalid document format in operation: " + doc);
                }
            }
            return result ? (operationId +":" +leaderInfo ): null;
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
        
    }

    // SPRINT 3 IMP
    public void syncNewElement(){
        // 1st prepare the message for multicast discovery
        int port_for_syncing = 9999;
        Message discoveryMessage = Message.discoveryMessage(this.gossipNode.getNodeId(), port_for_syncing);
        //Function to send multicast asking who is the leader
        String LeaderACK = broadcastDiscovery(port_for_syncing, discoveryMessage);

        String[] parts = LeaderACK.split(":");
        String leaderID = parts[0];
        String leader_port = parts[1];
        
        fullSyncInnit(UUID.fromString(leaderID), Integer.parseInt(leader_port), port_for_syncing );




        /* pontos 1 e 2, 3 , e 4  feito falta o resto
          Funções a criar: 

          2- New element manda multicast averiguar lider ---> broadcast()  LINHA 111 FUNCAO E 95 EXEMPLO CHAMADA
          4- Sync request
          5- Sync()
          6- Comparar local bd do node com lider
          7- Pedir sync outra vez (dependendo)
        */ 

    }
    

    private void detectFailures() {
        long currentTime = System.currentTimeMillis();
        System.out.println(getName());
        //System.out.println("detecting failures");
        // gets all the sets in the map of nodes timestamps
        for (Map.Entry<String, Long> entry : lastReceivedHeartbeats.entrySet()) {
            String nodeId = entry.getKey();
            long lastReceivedTime = entry.getValue();

            if (currentTime - lastReceivedTime > FAILURE_TIMEOUT) {
                System.out.println("Node " + nodeId + " is considered failed.");
            }
        }
    }

    // Shut down the scheduler and socket when the thread stops
    @Override
    public void interrupt() {
        super.interrupt();
        scheduler.shutdown();
        socket.close();
    }

    public Map<UUID, AtomicInteger> getHeartbeatCounters() {
        return heartbeatCounters;
    }

    public Map<String, Long> getLastReceivedHeartbeats() {
        return lastReceivedHeartbeats;
    }






    
    /*
  _   _ ______ _________          ______  _____  _  _______ _   _  _____ 
 | \ | |  ____|__   __\ \        / / __ \|  __ \| |/ /_   _| \ | |/ ____|
 |  \| | |__     | |   \ \  /\  / / |  | | |__) | ' /  | | |  \| | |  __ 
 | . ` |  __|    | |    \ \/  \/ /| |  | |  _  /|  <   | | | . ` | | |_ |
 | |\  | |____   | |     \  /\  / | |__| | | \ \| . \ _| |_| |\  | |__| |
 |_| \_|______|  |_|      \/  \/   \____/|_|  \_\_|\_\_____|_| \_|\_____|
                                                                         
                                                                         

    */
    /** FOR IMPROVE READABILITY IN THE BROADCAST AND UNICAST ABOVE */
    private byte[] serialize(Object object) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (ObjectOutputStream objectStream = new ObjectOutputStream(byteStream)) {
            objectStream.writeObject(object);
            objectStream.flush();
        }
        return byteStream.toByteArray();
    }

    private Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        try (ObjectInputStream objectStream = new ObjectInputStream(new ByteArrayInputStream(data))) {
            return objectStream.readObject();
        }
    }
    

    private byte[] addHeader(String header, byte[] payload) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        byteStream.write(header.getBytes()); 
        byteStream.write(payload);           
        return byteStream.toByteArray();
    }










































































































    

    // get the IP of a node
    private String getNodeIPAddress(UUID nodeId) {
        //static beacuse running on local machine
        return "localhost";
    }

 
    
}

