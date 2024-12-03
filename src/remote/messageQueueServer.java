package remote;


import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;


import shared.MessageQueue;


public class messageQueueServer extends Thread {       

    public Registry registry;
    private MessageQueue mq;
    private int registryPort;
    //private String regName;
    private messageRemoteInterface stub;

    //private volatile boolean running;
    
    public messageQueueServer(String NodeID, int port) throws RemoteException{
        //regName = NodeID;
        registryPort = port;
        //running = true;
        mq = new MessageQueue();
        stub = (messageRemoteInterface) UnicastRemoteObject.exportObject(mq, port);
    }

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

    

    public boolean checkQueue() throws RemoteException{
        return !mq.isEmpty();
    }

    public MessageQueue getQueue (){
        return mq;
    }

}