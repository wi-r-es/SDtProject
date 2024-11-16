package Services;
import Nodes.*;
import Resources.Document;
import shared.CompressionUtils;
import shared.Message;
import shared.OPERATION;

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

public class HeartbeatService extends Thread {

    private final GossipNode gossipNode;
    private final Map<UUID, AtomicInteger> heartbeatCounters;  // heartbeat counter for each node
    private final Map<String, Long> lastReceivedHeartbeats;  // last received heartbeat timestamps
    private final ScheduledExecutorService scheduler; // for running heartbeats regularly [and fail detection in the future]
    private DatagramSocket socket;

    private static final int HEARTBEAT_INTERVAL = 5000;  // Interval in milliseconds for sending heartbeats
    private static final int FAILURE_TIMEOUT = 10000;  // Timeout to detect failure (ms)

    private static final int NODE_PORT_BASE = 5000;  // base port for UDP communication

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

            this.socket = new DatagramSocket();
            // this.socket.setSoTimeout(HEARTBEAT_INTERVAL);
            
            //GOSSIP PROTO
            //this.socket = new DatagramSocket(NODE_PORT_BASE + Math.abs(gossipNode.getNodeId().hashCode()) % 1000);  // Unique port 
           
        } catch (SocketException e) {
            e.printStackTrace();
        }

        heartbeatCounters.put(gossipNode.getNodeId(), new AtomicInteger(0));  // Initializes heartbeat counter
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
            Message hb_message = Message.heartbeatMessage("Heartbeat:" + gossipNode.getNodeId() + ":" + MULTICAST_GROUP + ":" + incrementHeartbeat()); 
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
    private void broadcast(Message message, boolean compress) {
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

    private void respondeToHeartbeat(UUID targetNodeId) {
        try {
            Message msg = Message.replyHeartbeatMessage("ACK_Heartbeat:" + gossipNode.getNodeId() + ":" + heartbeatCounters.get(gossipNode.getNodeId()).getAndIncrement() + ":" + System.currentTimeMillis());
            byte[] serializedData = serialize(msg);
            byte[] finalData = addHeader("UNCO", serializedData);
                
            InetAddress targetAddress = InetAddress.getByName(getNodeIPAddress(targetNodeId));
            int targetPort = NODE_PORT_BASE; 

            DatagramPacket packet = new DatagramPacket(finalData, finalData.length, targetAddress, targetPort);
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
        if(gossipNode.getNodeId().equals(UUID.fromString(senderNodeId))) { return;}
        int heartbeatCounter = Integer.parseInt(parts[3]);

        // Update local heartbeat data

        //computeIfAbsente looks for an entry in "heartbeatCounters" with the key senderNodeId.
        // if key not present, creates a nwe entry with amoticInteger initialization with initialValue = 0
        // updateandGet will atomically updated the value retrieved from before 
        heartbeatCounters.computeIfAbsent(UUID.fromString(senderNodeId), k -> new AtomicInteger(0))
            .updateAndGet(current -> Math.max(current, heartbeatCounter));
        lastReceivedHeartbeats.put(senderNodeId, System.currentTimeMillis());
        
        respondeToHeartbeat(UUID.fromString(senderNodeId));        
    }
    private void addKnownNode(Message message){
        Object obj = message.getPayload();
        String content = (String) obj;
        System.out.println("\tCotent inside add to knownNodes->  " + content+"\n\n");
        String[] parts = content.split(":");
        String senderNodeId = parts[1];

        gossipNode.addKnownNode(UUID.fromString(senderNodeId));
    }
    

    private void receiveMulticast() { // ADD RESPONT TO HEARTBEATS
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
                    Document doc = (Document) obj;
                        
                    switch (op) {
                        case SYNC: //for syncing purposes
                                
                            break;
                        
                        case COMMIT: // for commit purposes
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
                        case DISCOVERY: // for NEW NODE 
                            if (this.gossipNode.isLeader()){
                                // logic to reply
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
            //socket.setSoTimeout(5000);
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    socket.receive(packet); 
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
                        Document doc = (Document) obj;
                            
                        switch (op) {
                            case FULL_SYNC: //for full sync for new node
                                    
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
                            
                            case ACK: // for NEW NODE 
                                if (this.gossipNode.isLeader()){
                                    // logic to reply
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




    private void sendSyncAck(Message msg) {
        try (
            Socket tcpS = new Socket("localhost", TCP_PORT);
            ObjectOutputStream out = new ObjectOutputStream(tcpS.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(tcpS.getInputStream())
        ) {
            // Send the custom Message object
            out.writeObject(msg);
            // Receive the response from the server
            Message response = (Message) in.readObject();
            System.out.println("Server Response: " + response);

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    


    // SPRINT 3 IMP
    private void syncNewElement(){
        /*
          Funções a criar: 

          1- Entrar em grupo
          2- New element manda multicast averiguar lider ---> broadcast()  LINHA 111 FUNCAO E 95 EXEMPLO CHAMADA
          3- Unicast lider resposta --> Message performOperation(OPERATION operation, Object data) throws RemoteException; RMI
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






    
    //NETWORKING
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

