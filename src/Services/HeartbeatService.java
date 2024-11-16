package Services;
import Nodes.*;

import java.io.IOException;
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

    private static final int HEARTBEAT_INTERVAL = 1000;  // Interval in milliseconds for sending heartbeats
    private static final int FAILURE_TIMEOUT = 10000;  // Timeout to detect failure (ms)

    private static final int NODE_PORT_BASE = 5000;  // base port for UDP communication

    private static final int PORT = 9876;  // UDP communication multicast
    private static final String MULTICAST_GROUP = "230.0.0.0";  

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
        
        // gossip protocol way
        //scheduler.scheduleAtFixedRate(this::incrementAndGossipHeartbeat, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
        
        
        // broadcast way
        scheduler.scheduleAtFixedRate(this::incrementAndBroadcastHeartbeat, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);


        // Detection failure
        //scheduler.scheduleAtFixedRate(this::detectFailures, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);

        // Start a separate thread for continuously receiving heartbeats
        new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                //receiveHeartbeats();
                receiveHeartbeatsBroadcast();
            }
        }).start();
        new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                receiveACK();
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
            this.broadcastHeartbeat();}

    }

    private int getHeartbeatCounter() {
        return heartbeatCounters.get(gossipNode.getNodeId()).get();
    }
    private void incrementHeartbeat() {
        heartbeatCounters.get(gossipNode.getNodeId()).incrementAndGet();
    }

    
    //BROADCAST
    private void broadcastHeartbeat() {
        try {
            String message = gossipNode.getNodeId() + ":" + heartbeatCounters.get(gossipNode.getNodeId()).getAndIncrement();
            byte[] buffer = message.getBytes();

            InetAddress group = InetAddress.getByName(MULTICAST_GROUP);
            DatagramPacket pckt = new DatagramPacket(buffer, buffer.length, group, PORT);
            socket.send(pckt);
            

            
            System.out.println("Sending heartbeat from " + gossipNode.getNodeId() + " to " + MULTICAST_GROUP + " with counter " + getHeartbeatCounter());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void receiveHeartbeatsBroadcast() {
        try {
            MulticastSocket mSocket = new MulticastSocket(PORT);
            InetAddress group = InetAddress.getByName(MULTICAST_GROUP);
            mSocket.joinGroup(group);
            byte[] buffer = new byte[256];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            mSocket.receive(packet);

            String receivedMessage = new String(packet.getData(), 0, packet.getLength());
            String[] parts = receivedMessage.split(":");
            String senderNodeId = parts[0];
            int heartbeatCounter = Integer.parseInt(parts[1]);
            if(gossipNode.getNodeId().equals(senderNodeId)) { return;}
            respondeToHeartbeat(UUID.fromString(senderNodeId));
            // update the heartbeat information for the sender node
            heartbeatCounters.computeIfAbsent(UUID.fromString(senderNodeId), k -> new AtomicInteger(0))
                    .updateAndGet(current -> Math.max(current, heartbeatCounter));
            lastReceivedHeartbeats.put(senderNodeId, System.currentTimeMillis());

            System.out.println("Received heartbeat from " + senderNodeId + " with counter " + heartbeatCounter);
        } catch (IOException e) {
            if (!(e instanceof SocketTimeoutException)) {
                e.printStackTrace();  // Ignore timeout exceptions (normal in receive loop)
            }
        }
    }

    private void respondeToHeartbeat(UUID targetNodeId) {
        try {
                String message = "ACK Heartbeat from:" +gossipNode.getNodeId() + ":" + heartbeatCounters.get(gossipNode.getNodeId()).getAndIncrement() + ":" + System.currentTimeMillis();
                byte[] buffer = message.getBytes();

                InetAddress targetAddress = InetAddress.getByName(getNodeIPAddress(targetNodeId));
                int targetPort = NODE_PORT_BASE; 

                DatagramPacket packet = new DatagramPacket(buffer, buffer.length, targetAddress, targetPort);
                socket.send(packet);

                System.out.println("ACK FROM " + gossipNode.getNodeId() + " to " + targetNodeId + " with counter " + getHeartbeatCounter());
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    private void receiveACK() {
        byte[] buffer = new byte[256];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

        try {
            socket.receive(packet); 
            String receivedMessage = new String(packet.getData(), 0, packet.getLength());

            // Parse sender ID and heartbeat counter from message
            String[] parts = receivedMessage.split(":");
            System.out.println(receivedMessage);
            String senderNodeId = parts[1];
            int heartbeatCounter = Integer.parseInt(parts[2]);
            long timestamp = Long.parseLong(parts[3]); //what in the hell will i use this for idk but we'll see

            // Update local heartbeat data

            //computeIfAbsente looks for an entry in "heartbeatCounters" with the key senderNodeId.
            // if key not present, creates a nwe entry with amoticInteger initialization with initialValue = 0
            // updateandGet will atomically updated the value retrieved from before 
            heartbeatCounters.computeIfAbsent(UUID.fromString(senderNodeId), k -> new AtomicInteger(0))
                    .updateAndGet(current -> Math.max(current, heartbeatCounter));
            lastReceivedHeartbeats.put(senderNodeId, System.currentTimeMillis());


            //Add node to the list
            gossipNode.addKnownNode(UUID.fromString(senderNodeId));

        } catch (IOException e) {
            if (!(e instanceof SocketTimeoutException)) {
                e.printStackTrace();  // Ignore timeout exceptions (normal in receive loop)
            }
        }
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









































































































// GOSSIP


    // GOSSIP protocol implementation
    private void incrementAndGossipHeartbeat() {
        incrementHeartbeat();
        gossipHeartbeat();
    }
    // heartbeat
    private void gossipHeartbeat() {
        List<UUID> targetNodeIds = gossipNode.getRandomNodes();  // Get target node IDs from Node

        for (UUID targetNodeId : targetNodeIds) {
            try {
                String message = gossipNode.getNodeId() + ":" + getHeartbeatCounter() + ":" + System.currentTimeMillis();
                byte[] buffer = message.getBytes();

                InetAddress targetAddress = InetAddress.getByName(getNodeIPAddress(targetNodeId));
                int targetPort = NODE_PORT_BASE;  

                DatagramPacket packet = new DatagramPacket(buffer, buffer.length, targetAddress, targetPort);
                socket.send(packet);

                System.out.println("Sending heartbeat from " + gossipNode.getNodeId() + " to " + targetNodeId + " with counter " + getHeartbeatCounter());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    

    // get the IP of a node
    private String getNodeIPAddress(UUID nodeId) {
        //static beacuse running on local machine
        return "localhost";
    }

    private void receiveHeartbeats() {
        byte[] buffer = new byte[256];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

        try {
            socket.receive(packet);  // Receive heartbeat packet
            String receivedMessage = new String(packet.getData(), 0, packet.getLength());

            // Parse sender ID and heartbeat counter from message
            String[] parts = receivedMessage.split(":");
            String senderNodeId = parts[0];
            int heartbeatCounter = Integer.parseInt(parts[1]);
            long timestamp = Long.parseLong(parts[2]); //what in the hell will i use this for idk but we'll see

            // Update local heartbeat data

            //computeIfAbsente looks for an entry in "heartbeatCounters" with the key senderNodeId.
            // if key not present, creates a nwe entry with amoticInteger initialization with initialValue = 0
            // updateandGet will atomically updated the value retrieved from before 
            heartbeatCounters.computeIfAbsent(UUID.fromString(senderNodeId), k -> new AtomicInteger(0))
                    .updateAndGet(current -> Math.max(current, heartbeatCounter));
            lastReceivedHeartbeats.put(senderNodeId, System.currentTimeMillis());

            // Log receiving heartbeat
            System.out.println("Received heartbeat from " + senderNodeId + " with counter " + heartbeatCounter);
        } catch (IOException e) {
            if (!(e instanceof SocketTimeoutException)) {
                e.printStackTrace();  // Ignore timeout exceptions (normal in receive loop)
            }
        }
    }
    
}

