package shared;
import java.rmi.RemoteException;
import java.util.concurrent.ConcurrentLinkedQueue;

import Resources.Document;
import remote.messageRemoteInterface;

/**
 * Represents a message queue that allows enqueuing and dequeuing of messages.
 * Implements the messageRemoteInterface for remote method invocation.
 */
public class MessageQueue implements messageRemoteInterface{

    // or BlockingQueue = ArrayBlockingQueue / LinkedBlockingQueue //
    private final ConcurrentLinkedQueue<Message> queue = new ConcurrentLinkedQueue<>();

    /**
     * Constructs a new message queue.
     *
     * @throws RemoteException If a remote exception occurs.
     */
    public MessageQueue() throws RemoteException {
        super();
    
    }

    

    /**
     * Enqueues a message into the queue.
     *
     * @param message The message to enqueue.
     * @throws RemoteException If a remote exception occurs.
     */
    public void enqueue(Message message) throws RemoteException {
        queue.add(message);
        System.out.println("Enqueued: " + message.toString());
    }
    /**
     * Dequeues a message from the queue.
     *
     * @return The dequeued message, or null if the queue is empty.
     * @throws RemoteException If a remote exception occurs.
     */
    public Message dequeue() throws RemoteException {
        Message message = queue.poll();
        if (message != null) {
            System.out.println("Dequeued: " + message.toString());
        }
        return message;
    }

    /**
     * Checks if the queue is empty.
     *
     * @return true if the queue is empty, false otherwise.
     * @throws RemoteException If a remote exception occurs.
     */
    public boolean isEmpty() throws RemoteException{
        return queue.isEmpty();
    }


    /**
     * Performs the specified operation on the given data.
     *
     * @param operation The operation to perform.
     * @param data      The data to operate on.
     * @return The message resulting from the operation.
     * @throws RemoteException If a remote exception occurs or if the operation is unsupported.
     */
    @Override
    public Message performOperation(OPERATION operation, Object data) throws RemoteException {
        switch (operation) {
            case CREATE:
                return createOperation(data);
            case UPDATE:
                return updateOperation(data);
            case DELETE:
                return deleteOperation(data);
            default:
                throw new RemoteException("Unsupported operation: " + operation);
        }
    }

    // Private methods for specific operations (createOperation, updateOperation, deleteOperation)
    private Message createOperation(Object data) {
        if (data instanceof Document || data instanceof String) {
            Message msg= new Message(OPERATION.CREATE, data);
            System.out.println("Message created succesfully");
            return msg;
        }
        System.err.println("Invalid data for CREATE operation.");
        return null ;
    }
    private Message updateOperation(Object data) {
        if (data instanceof Document || data instanceof String) {
            Document doc = (Document) data;
            doc.incVersion();
            Message msg;
            try {
                msg = new Message(OPERATION.UPDATE, Document.clone(doc) );
                System.out.println("Message created succesfully");
            return msg;
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
            
        }
        System.err.println("Invalid data for UPDATE operation.");
        return null ;
    }
    private Message deleteOperation(Object data) {
        if (data instanceof Document || data instanceof String) {
            Message msg= new Message(OPERATION.DELETE, data);
            System.out.println("Message created succesfully");
            return msg;
        }
        System.err.println("Invalid data for DELETE operation.");
        return null ;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("{MESSAGE QUEUE: ");
        for (Message msg : queue){
            sb.append("[MESSAGE]: " + msg.toString());
            sb.append("##");
        }
        sb.append("}");
        return sb.toString();
    }
    

}
