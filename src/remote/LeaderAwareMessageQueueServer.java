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

/**
 * LeaderAwareMessageQueue is an enhanced remote interface that extends the messageRemoteInterface.
 * It includes methods to retrieve the current leader and check if the current node is the leader.
 * @see remote.messageRemoteInterface
 */
interface LeaderAwareMessageQueue extends messageRemoteInterface {
    /**
     * Returns the UUID of the current leader node.
     *
     * @return The UUID of the current leader node.
     * @throws RemoteException If a remote exception occurs.
     */
    UUID getCurrentLeader() throws RemoteException;
    /**
     * Checks if the current node is the leader.
     *
     * @return true if the current node is the leader, false otherwise.
     * @throws RemoteException If a remote exception occurs.
     */
    boolean isLeader() throws RemoteException;
}

/**
 * LeaderAwareMessageQueueServer is a leader-aware implementation of the message queue.
 * It extends the messageQueueServer and implements the LeaderAwareMessageQueue interface.
 * The class includes methods to enqueue, dequeue, and perform operations on messages, ensuring that
 * these actions are only executed if the current node is the leader. It also provides methods to
 * retrieve the current leader and transfer ownership of the message queue to a new leader.
 * @see remote.messageQueueServer
 * @see remote.LeaderAwareMessageQueue
 */
public class LeaderAwareMessageQueueServer extends messageQueueServer implements LeaderAwareMessageQueue {
    private volatile UUID currentLeader;
    private final RaftNode raftNode;  // Reference to RaftNode

    /**
     * Constructs a LeaderAwareMessageQueueServer instance.
     *
     * @param NodeID  The ID of the node.
     * @param port    The port number for the server.
     * @param nodeId  The UUID of the node.
     * @param raftNode The RaftNode instance associated with the server.
     * @throws RemoteException If a remote exception occurs.
     */
    public LeaderAwareMessageQueueServer(String NodeID, int port, UUID nodeId, RaftNode raftNode) throws RemoteException {
        super(NodeID, port);
        //this.nodeId = nodeId;
        this.raftNode = raftNode;
        this.currentLeader = nodeId; // Initially assume we're leader
    }

    /**
     * Returns the UUID of the current leader node.
     *
     * @return The UUID of the current leader node.
     * @throws RemoteException If a remote exception occurs.
     */
    @Override
    public UUID getCurrentLeader() throws RemoteException {
        return currentLeader;
    }

    /**
     * Checks if the current node is the leader.
     *
     * @return true if the current node is the leader, false otherwise.
     * @throws RemoteException If a remote exception occurs.
     */
    @Override
    public boolean isLeader() throws RemoteException {
        return raftNode.isLeader();
    }

    /**
     * Enqueues a message to the message queue.
     *
     * @param message The message to enqueue.
     * @throws RemoteException If a remote exception occurs.
     * @throws LeaderRedirectException If the current node is not the leader.
     */
    @Override
    public void enqueue(Message message) throws RemoteException {
        if (!isLeader()) {
            throw new LeaderRedirectException(currentLeader);
        }
        super.getQueue().enqueue(message);
    }

    /**
     * Dequeues a message from the message queue.
     *
     * @return The dequeued message.
     * @throws RemoteException If a remote exception occurs.
     * @throws LeaderRedirectException If the current node is not the leader.
     */
    @Override
    public Message dequeue() throws RemoteException {
        if (!isLeader()) {
            throw new LeaderRedirectException(currentLeader);
        }
        return super.getQueue().dequeue();
    }

    /**
     * Performs an operation on the message queue.
     *
     * @param operation The operation to perform.
     * @param data      The data associated with the operation.
     * @return The result of the operation.
     * @throws RemoteException If a remote exception occurs.
     * @throws LeaderRedirectException If the current node is not the leader.
     */
    @Override
    public Message performOperation(OPERATION operation, Object data) throws RemoteException {
        if (!isLeader()) {
            throw new LeaderRedirectException(currentLeader);
        }
        return super.getQueue().performOperation(operation, data);
    }

    /**
     * Transfers ownership of the message queue to a new leader. 
     *
     * @param newOwner The UUID of the new leader node.
     * @throws RuntimeException If the ownership transfer fails.
     */
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

    /**
     * The MessageQueueClient class provides a client-side interface for interacting with a remote message queue.
     * It handles the connection to the registry, error handling, and retrying of operations in case of failures.
     * @see remote.messageRemoteInterface
     */
    public static class MessageQueueClient {
        private Registry registry;
        private messageRemoteInterface queue;
        private int maxRetries = 3;
        private int port;
    
        /**
         * Constructs a MessageQueueClient instance with the specified port.
         *
         * @param port The port number to connect to the registry.
         */
        public MessageQueueClient(int port) {
            this.port = port;
        }
    
        /**
         * Connects to the registry and looks up the message queue.
         * Retries the connection if it fails, up to a maximum number of attempts.
         *
         * @throws RemoteException If the connection fails after the maximum number of attempts.
         */
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
    
        /**
         * Enqueues a message to the remote message queue.
         * Handles leader redirection and retries the operation if it fails.
         *
         * @param message The message to enqueue.
         * @throws RemoteException If the operation fails after the maximum number of attempts.
         * @throws InterruptedException If the thread is interrupted while waiting between retries.
         * @see remote.LeaderAwareMessageQueueServer.MessageQueueClient#connectToRegistry()
         */
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
        /**
         * Enqueues a message to the remote message queue by performing the specified operation on the given data.
         * Handles leader redirection and retries the operation if it fails.
         *
         * @param operation The operation to perform.
         * @param data The data to perform the operation on.
         * @throws RemoteException If the operation fails after the maximum number of attempts.
         * @throws InterruptedException If the thread is interrupted while waiting between retries.
         * @see remote.LeaderAwareMessageQueueServer.MessageQueueClient#connectToRegistry()
         */
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

        /**
         * Dequeues a message from the remote message queue.
         * Handles leader redirection and retries the operation if it fails.
         *
         * @return The dequeued message.
         * @throws RemoteException If the operation fails after the maximum number of attempts.
         * @see remote.LeaderAwareMessageQueueServer.MessageQueueClient#connectToRegistry()
         */
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
