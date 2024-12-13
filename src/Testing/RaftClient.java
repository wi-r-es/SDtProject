package Testing;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

import java.rmi.RemoteException;

import Nodes.Raft.RaftNode;
import Resources.Document;
import remote.LeaderAwareMessageQueueServer;

import shared.OPERATION;

public class RaftClient {
    public static void main(String[] args) throws InterruptedException {
        try {

            try {
                File logFile = new File("raftClientTest.txt");
                PrintStream fileOut = new PrintStream(logFile);
                System.setOut(fileOut); // Redirects System.out to the file
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return; // Exit if the file can't be created
            }

            LeaderAwareMessageQueueServer.MessageQueueClient client = new LeaderAwareMessageQueueServer.MessageQueueClient(2323);

            Document doc1 = new Document("This is a new document1");
            Document doc2 = new Document("This is a new document2");
            Document doc3 = new Document("This is a new document3");
            //Message msg = new Message(OPERATION.CREATE, doc1);
            // Perform remote operations
            //rq.enqueue(msg);
            // Use the client
            try {
                // Enqueue message
                
                System.out.println("Attempting to enqueue message...");

                client.enqueue(OPERATION.CREATE, doc1);
                client.enqueue(OPERATION.UPDATE, doc1);
                client.enqueue(OPERATION.CREATE, doc2);
                client.enqueue(OPERATION.CREATE, doc2);
                client.enqueue(OPERATION.CREATE, doc3);
                client.enqueue(OPERATION.DELETE, doc3);

                Thread.sleep(10000);
                RaftNode node = new RaftNode("Node-66", false, true);
              
                System.out.println("Ffull sync new node");
                node.start();
                node.getGossipNode().getHeartbeatService().syncNewElementRaftCluster();

                Document doc4 = new Document("This is a new document4");
                Document doc5 = new Document("This is a new document5");
                Document doc6 = new Document("This is a new document6");
                doc1.setContent("Updated doc 1");
                // System.out.println("Second Batch of operations");

                client.enqueue(OPERATION.CREATE, doc4);
                client.enqueue(OPERATION.UPDATE, doc1);
                client.enqueue(OPERATION.CREATE, doc5);
                client.enqueue(OPERATION.CREATE, doc6);
                client.enqueue(OPERATION.DELETE, doc2);

                System.out.println("Message enqueued successfully");
                
                // Dequeue message
                //Message received = client.dequeue();
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        
    }


}




