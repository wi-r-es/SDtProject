package shared;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentLinkedQueue;

import Resources.Document;
import remote.messageRemoteInterface;


public class MessageQueue extends UnicastRemoteObject implements messageRemoteInterface{


    public MessageQueue() throws RemoteException {
        super();
    
    }

    // or BlockingQueue = ArrayBlockingQueue / LinkedBlockingQueue //
    private final ConcurrentLinkedQueue<Message> queue = new ConcurrentLinkedQueue<>();

    /**
     * method to add message to queue
     */
    public void enqueue(Message message) throws RemoteException {
        queue.add(message);
        System.out.println("Enqueued: " + message.toString());
    }
    /**
     * method to remove message from queue
     */
    public Message dequeue() throws RemoteException {
        Message message = queue.poll();
        if (message != null) {
            System.out.println("Dequeued: " + message.toString());
        }
        return message;
    }

    /**
     * Method to check if the queue is empty
     * @return
     */
    public boolean isEmpty() throws RemoteException{
        return queue.isEmpty();
    }


    /**
     * Method to perform operations precoded
     *  */ 
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
    







    // @Override
    // public boolean sync(Message message) throws RemoteException {
    //     mq.add(message);
    //     return true;
    // }
    
}
