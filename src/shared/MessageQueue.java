package shared;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentLinkedQueue;

import remote.messageRemoteInterface;


public class MessageQueue extends UnicastRemoteObject implements messageRemoteInterface{


    public MessageQueue() throws RemoteException {
        super();
    
    }

    // or BlockingQueue = ArrayBlockingQueue / LinkedBlockingQueue
    private final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();

    // method to add message to queue
    public void enqueue(String message) throws RemoteException {
        queue.add(message);
        System.out.println("Enqueued: " + message);
    }
    // method to remove message from queue
    public String dequeue() throws RemoteException {
        String message = queue.poll();
        if (message != null) {
            System.out.println("Dequeued: " + message);
        }
        return message;
    }

    // Method to check if the queue is empty
    public boolean isEmpty() {
        return queue.isEmpty();
    }
    
}
