

package Nodes;
import Documents.*;

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

public class NodeThread extends Node{
    private final String nodeId;
    private final GossipNode gossipNode;  // Handles gossip protocol aspects of this node
    private final Set<String> knownNodes = new HashSet<>();  // Known node IDs (dynamic list)

    private static final String DISCOVERY_SERVER_HOST = "localhost";
    private static final int DISCOVERY_SERVER_PORT = 9090;


    private Map<String, Document> documents;
    // Other components, e.g., DocumentManager, CRDT handler, etc.

    public NodeThread(String nodeId) {
        this.nodeId = nodeId;
        this.gossipNode = new GossipNode(this);  // Initialize gossip component by passing 'this' node to 'gossipnode'
        this.documents = new HashMap<>();

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

        // Start the gossip protocol
        gossipNode.start();
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
}

// public class Node {
//     private final String nodeId;
//     private final Map<String, Document> documents;

//     public Node(String nodeId) {
//         this.nodeId = nodeId;
//         this.documents = new HashMap<>();
//     }

//     public String getNodeId() {
//         return nodeId;
//     }

//     public void addDocument(Document document) {
//         documents.put(document.getId(), document);
//     }

//     public Document getDocument(String documentId) {
//         return documents.get(documentId);
//     }

//     public void updateDocument(String documentId, String newContent) {
//         Document doc = documents.get(documentId);
//         if (doc != null) {
//             doc.setContent(newContent);
//         }
//     }
// }
