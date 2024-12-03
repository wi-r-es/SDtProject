package remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.UUID;

import Exception.LeaderRedirectException;
import Nodes.Raft.RaftNode;
import shared.Message;
import shared.OPERATION;

    // Enhanced Remote Interface with leader awareness
interface LeaderAwareMessageQueue extends messageRemoteInterface {
    UUID getCurrentLeader() throws RemoteException;
    boolean isLeader() throws RemoteException;
}

// Leader-aware implementation of the message queue
public class LeaderAwareMessageQueueServer extends messageQueueServer implements LeaderAwareMessageQueue {
    //private final UUID nodeId;
    private volatile UUID currentLeader;
    private final RaftNode raftNode;  // Reference to RaftNode

    public LeaderAwareMessageQueueServer(String NodeID, int port, UUID nodeId, RaftNode raftNode) throws RemoteException {
        super(NodeID, port);
        //this.nodeId = nodeId;
        this.raftNode = raftNode;
        this.currentLeader = nodeId; // Initially assume we're leader
    }

    @Override
    public UUID getCurrentLeader() throws RemoteException {
        return currentLeader;
    }

    @Override
    public boolean isLeader() throws RemoteException {
        return raftNode.isLeader();
    }

    @Override
    public void enqueue(Message message) throws RemoteException {
        if (!isLeader()) {
            throw new LeaderRedirectException(currentLeader);
        }
        super.getQueue().enqueue(message);
    }

    @Override
    public Message dequeue() throws RemoteException {
        if (!isLeader()) {
            throw new LeaderRedirectException(currentLeader);
        }
        return super.getQueue().dequeue();
    }

    @Override
    public Message performOperation(OPERATION operation, Object data) throws RemoteException {
        if (!isLeader()) {
            throw new LeaderRedirectException(currentLeader);
        }
        return super.getQueue().performOperation(operation, data);
    }

    @Override
    public void transferOwnership(String newOwner) {
        try {
            // Update leader information
            UUID newLeaderId = UUID.fromString(newOwner);
            this.currentLeader = newLeaderId;
            
            if (registry != null) {
                // Rebind the queue with new ownership
                registry.rebind("MessageQueue", UnicastRemoteObject.exportObject(this, 0));
                System.out.println("MessageQueue ownership transferred to " + newOwner);
            }
        } catch (RemoteException e) {
            throw new RuntimeException("Failed to transfer ownership", e);
        }
    }


    public static class MessageQueueClient {
        private Registry registry;
        private messageRemoteInterface queue;
        private int maxRetries = 3;
        private int port;
    
        public MessageQueueClient(int port) {
            this.port = port;
        }
    
        private void connectToRegistry() throws RemoteException {
            int attempts = 0;
            while (attempts < maxRetries) {
                try {
                    System.setProperty("java.rmi.server.hostname", "localhost");
                    registry = LocateRegistry.getRegistry("localhost", port);
                    
                    // Try to lookup the queue
                    queue = (messageRemoteInterface) registry.lookup("MessageQueue");
                    System.out.println("Successfully connected to MessageQueue on port " + port);
                    return;
                    
                } catch (Exception e) {
                    attempts++;
                    if (attempts >= maxRetries) {
                        throw new RemoteException("Failed to connect to message queue after " + maxRetries + " attempts", e);
                    }
                    try {
                        Thread.sleep(1000);
                        System.out.println("Retrying connection... Attempt " + attempts);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    
        public void enqueue(Message message) throws RemoteException, InterruptedException {
            int attempts = 0;
            while (attempts < maxRetries) {
                try {
                    if (queue == null) {
                        connectToRegistry();
                    }
                    queue.enqueue(message);
                    return;
                } catch (LeaderRedirectException e) {
                    // Reconnect to new leader
                    queue = null;
                    attempts++;
                    Thread.sleep(1000); // Wait before retry
                } catch (RemoteException e) {
                    queue = null;
                    attempts++;
                    if (attempts == maxRetries) {
                        throw e;
                    }
                    Thread.sleep(1000); // Wait before retry
                }
            }
            throw new RemoteException("Failed to enqueue after " + maxRetries + " attempts");
        }
        public void enqueue(OPERATION operation, Object data) throws RemoteException, InterruptedException {
            int attempts = 0;
            while (attempts < maxRetries) {
                try {
                    if (queue == null) {
                        connectToRegistry();
                    }
                    queue.enqueue(queue.performOperation(operation, data));
                    return;
                } catch (LeaderRedirectException e) {
                    // Reconnect to new leader
                    queue = null;
                    attempts++;
                    Thread.sleep(1000); // Wait before retry
                } catch (RemoteException e) {
                    queue = null;
                    attempts++;
                    if (attempts == maxRetries) {
                        throw e;
                    }
                    Thread.sleep(1000); // Wait before retry
                }
            }
            throw new RemoteException("Failed to enqueue after " + maxRetries + " attempts");
        }
    
        public Message dequeue() throws RemoteException {
            int attempts = 0;
            while (attempts < maxRetries) {
                try {
                    if (queue == null) {
                        connectToRegistry();
                    }
                    return queue.dequeue();
                } catch (LeaderRedirectException e) {
                    queue = null;
                    attempts++;
                } catch (RemoteException e) {
                    queue = null;
                    attempts++;
                    if (attempts == maxRetries) {
                        throw e;
                    }
                }
            }
            throw new RemoteException("Failed to dequeue after " + maxRetries + " attempts");
        }
        

    }
}
