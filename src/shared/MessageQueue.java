package shared;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentLinkedQueue;

import remote.messageRemote;

public class MessageQueue extends UnicastRemoteObject implements messageRemote{
    public MessageQueue() throws RemoteException {
        super();
    }

    // or BlockingQueue = ArrayBlockingQueue / LinkedBlockingQueue
    private final ConcurrentLinkedQueue<Message> queue = new ConcurrentLinkedQueue<>();

    // method to add message to queue
    public void enqueue(Message message) {
        queue.add(message);
        System.out.println("Enqueued: " + message);
    }
    // method to remove message from queue
    public Message dequeue() {
        Message message = queue.poll();
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
