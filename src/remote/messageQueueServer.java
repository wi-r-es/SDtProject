package remote;


import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;


import shared.MessageQueue;

/**
 * The messageQueueServer class represents a server that hosts a message queue and exposes it via RMI.
 * It extends the Thread class to run as a separate thread.
 * @see remote.messageRemoteInterface
 */
public class messageQueueServer extends Thread {       

    public Registry registry;
    private MessageQueue mq;
    private int registryPort;
    private messageRemoteInterface stub;

    /**
     * Constructs a new messageQueueServer instance.
     *
     * @param NodeID The ID of the node hosting the message queue server.
     * @param port The port number on which to expose the message queue via RMI.
     * @throws RemoteException If there is an error exporting the message queue stub.
     */
    public messageQueueServer(String NodeID, int port) throws RemoteException{
        //regName = NodeID;
        registryPort = port;
        //running = true;
        mq = new MessageQueue();
        stub = (messageRemoteInterface) UnicastRemoteObject.exportObject(mq, port);
    }

    /**
     * The run method is executed when the thread starts.
     * It sets up an RMI registry, binds the message queue stub to the registry, and keeps the server running until interrupted.
     */
    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::unreg));
        try {
            System.setProperty("java.rmi.server.hostname", "localhost");
            
            // Create or get registry
            try {
                registry = LocateRegistry.getRegistry(registryPort);
                registry.list(); // Test if registry exists
            } catch (RemoteException e) {
                registry = LocateRegistry.createRegistry(registryPort);
                System.out.println("Created new RMI registry on port " + registryPort);
            }

            // Bind the already exported stub
            registry.rebind("MessageQueue", stub);
            System.out.println("MessageQueue bound to registry as 'MessageQueue' on port " + registryPort);

            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(1000);
            }

        } catch (RemoteException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    /**
     * Unregisters the message queue from the RMI registry and unexports the remote objects.
     * This method is called when the server is shutting down.
     */
    public void unreg() {
        try {
            if (registry != null) {
                try {
                    registry.unbind("MessageQueue");
                } catch (Exception e) {
                    // Ignore if already unbound
                }
            }
            if (stub != null) {
                try {
                    UnicastRemoteObject.unexportObject(mq, true);
                } catch (Exception e) {
                    // Ignore if already unexported
                }
            }
            if (registry != null) {
                try {
                    UnicastRemoteObject.unexportObject(registry, true);
                } catch (Exception e) {
                    // Ignore if already unexported
                }
            }
            System.out.println("MessageQueue server shutdown complete");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Transfers ownership of the message queue to a new owner.
     *
     * @param newOwner The ID of the new owner node.
     * @throws RuntimeException If there is an error transferring ownership.
     */
    public void transferOwnership(String newOwner) {
        try {
            System.out.println("Transferring MessageQueue ownership to " + newOwner);
            registry.rebind("MessageQueue", mq);
            System.out.println("MessageQueue successfully transferred to " + newOwner);
        } catch (RemoteException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to transfer ownership", e);
        }
    }

    /**
     * Checks if the message queue is empty.
     *
     * @return true if the message queue is empty, false otherwise.
     * @throws RemoteException If there is an error checking the queue status.
     */
    public boolean checkQueue() throws RemoteException{
        return !mq.isEmpty();
    }

    /**
     * Returns the message queue instance.
     *
     * @return The MessageQueue instance.
     */
    public MessageQueue getQueue (){
        return mq;
    }

}