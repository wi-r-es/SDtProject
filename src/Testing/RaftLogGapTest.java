package Testing;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import Nodes.Raft.LogEntry;
import Nodes.Raft.RaftNode;
import Resources.Document;
import remote.LeaderAwareMessageQueueServer;
import shared.OPERATION;

public class RaftLogGapTest {
    public static void main(String[] args) {
        List<RaftNode> nodes = new ArrayList<>();
        
        // Create 5 nodes
        for (int i = 0; i < 5; i++) { 
            String nodeId = "Node-" + i;
            try {
                nodes.add(new RaftNode(nodeId, false, true));
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }
        try {
            File logFile = new File("raftReplicationTestingAndDebuggingMissingLogs.txt");
            PrintStream fileOut = new PrintStream(logFile);
            System.setOut(fileOut); // Redirects System.out to the file
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return; // Exit if the file can't be created
        }
    
        // Start all nodes
        nodes.forEach(node -> new Thread(node).start());
        Document doc1 = new Document("This is a new document1");
        Document doc2 = new Document("This is a new document2");
        Document doc3 = new Document("This is a new document3");
        Document doc4 = new Document("This is a new document4");
        Document doc5 = new Document("This is a new document5");
        Document doc6 = new Document("This is a new document6");
        try {
            // Wait for initial leader election
            //Thread.sleep(10000);
            
            // // Find leader and a follower
            // RaftNode leader = nodes.stream()
            //     .filter(RaftNode::isLeader)
            //     .findFirst()
            //     .orElseThrow();
                
            // RaftNode followerToSleep = nodes.stream()
            //     .filter(n -> !n.isLeader())
            //     .findFirst()
            //     .orElseThrow();
            
            // System.out.println("Leader is: " + leader.getNodeName());
            // System.out.println("Selected follower to sleep: " + followerToSleep.getNodeName());
            // Wait for leader election with retry
            RaftNode leader = null;
            RaftNode followerToSleep = null;
            int maxAttempts = 10;
            int attempt = 0;

            while ((leader == null || followerToSleep == null) && attempt < maxAttempts) {
                Thread.sleep(2000);
                attempt++;
                
                System.out.println("Attempt " + attempt + " to find leader and follower");
                
                Optional<RaftNode> potentialLeader = nodes.stream()
                    .filter(RaftNode::isLeader)
                    .findFirst();
                    
                if (potentialLeader.isPresent()) {
                    leader = potentialLeader.get();
                    Optional<RaftNode> potentialFollower = nodes.stream()
                        .filter(n -> !n.isLeader())
                        .findFirst();
                        
                    if (potentialFollower.isPresent()) {
                        followerToSleep = potentialFollower.get();
                        System.out.println("Found leader: " + leader.getNodeName());
                        System.out.println("Selected follower: " + followerToSleep.getNodeName());
                        break;
                    }
                }
            }

            if (leader == null || followerToSleep == null) {
                System.out.println("Failed to find leader and follower after " + maxAttempts + " attempts");
                return;
            }
            
            // Put follower to sleep
            followerToSleep.simulateSleep();
            
            LeaderAwareMessageQueueServer.MessageQueueClient client = new LeaderAwareMessageQueueServer.MessageQueueClient(2323);


            // Generate some transactions while follower is sleeping
            for (int i = 0; i < 5; i++) {
                if( i== 0){
                    client.enqueue(OPERATION.CREATE, doc1);
                    client.enqueue(OPERATION.UPDATE, doc1);
                    Thread.sleep(1000);
                    Thread.sleep(1000);
                }
                if( i== 2){
                    client.enqueue(OPERATION.CREATE, doc2);
                    client.enqueue(OPERATION.CREATE, doc2);
                    Thread.sleep(1000);
                    Thread.sleep(1000);
                }
                if( i== 4){
                    client.enqueue(OPERATION.CREATE, doc3);
                    client.enqueue(OPERATION.DELETE, doc3);
                    Thread.sleep(1000);
                    Thread.sleep(1000);
                }
                
                Thread.sleep(1000); // Wait a bit between transactions
            }
            
            // Wake up follower
            Thread.sleep(2000);
            followerToSleep.simulateWakeup();
            Thread.sleep(3000);
            doc1.setContent("Updated doc 1");

            client.enqueue(OPERATION.CREATE, doc4);
            client.enqueue(OPERATION.UPDATE, doc1);
            client.enqueue(OPERATION.CREATE, doc5);
            client.enqueue(OPERATION.CREATE, doc6);
            client.enqueue(OPERATION.DELETE, doc2);
            
            // Wait for log replication to catch up
            Thread.sleep(10000);
            
            // Print log states
            System.out.println("\nLeader logs: "+leader.getLogsAsString());
            System.out.println("\nPreviously sleeping follower logs: "+ followerToSleep.getLogsAsString());

            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}