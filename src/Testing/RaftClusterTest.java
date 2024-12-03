package Testing;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import Nodes.Raft.RaftNode;
public class RaftClusterTest {
    public static void main(String[] args) {
        List<RaftNode> nodes = new ArrayList<>();
        //nodes.add(new RaftNode("Node-0", true));
        for (int i = 0; i < 5; i++) { 
            String nodeId = "Node-" + i;
            try {
                nodes.add(new RaftNode(nodeId, false));
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }
    
        // Start all nodes
        //nodes.forEach(node -> new Thread(node::startElection).start());
        //nodes.forEach(node -> new Thread(node::scheduleElectionTimeout).start());
        
        try {
            File logFile = new File("raftReplicationTestingAndDebugging.txt");
            PrintStream fileOut = new PrintStream(logFile);
            System.setOut(fileOut); // Redirects System.out to the file
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return; // Exit if the file can't be created
        }

       

        
        nodes.forEach(node -> new Thread(node).start());

       
        //System.out.println("Program finished.");
    }
}
    



