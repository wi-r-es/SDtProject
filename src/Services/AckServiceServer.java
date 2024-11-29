package Services;
import java.io.*;

import java.net.*;

import java.util.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import shared.Message;
import shared.OPERATION;

/**
 * Server for receiving acks from other nodes TCP implemented
 * 
 * 
 * 
 * 
 */
public class AckServiceServer {
    private static final int PORT = 9090;  // Port for service
    private Map<UUID, Integer> ACK_counts;
    private final AtomicInteger numOfActiveNodes = new AtomicInteger(0);
    private List<Message> AckList;
    private final ExecutorService executorService;

    // pondering the idea of using a Logger in the future


    public AckServiceServer() {
        this.ACK_counts = Collections.synchronizedMap(new HashMap<>());;
        this.AckList = Collections.synchronizedList(new ArrayList<Message>());
        // Virtual thread-based executor
        executorService = Executors.newVirtualThreadPerTaskExecutor();
    }





    public void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("ACK server started on port " + PORT);


             // Virtual thread to continuously process messages from the list
             executorService.submit(this::processMessages);

             // Accept client connections and handle them with virtual threads
             while (true) {
                 Socket clientSocket = serverSocket.accept();
                 executorService.submit(() -> handleClient(clientSocket));
             }

        } catch (BindException e) {
            System.err.println("Port " + PORT + " is already in use. Choose another port.");
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Failed to start ACK server on port " + PORT);
            e.printStackTrace();
        }
    }

    private void handleClient(Socket clientSocket) {
        try (
            //BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
            ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream())
        ) {
            Message message = (Message) in.readObject();
            System.out.println("Received: " + message);
            synchronized(AckList) {AckList.add(message);}
            out.writeObject(new String("Server"+ "Acknowledged: " + message.getOperation() ));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
                    System.err.println("class not found exception");
                    e.printStackTrace();
                } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private synchronized void processMessages() {
        while (true) {
            try {
                if (!AckList.isEmpty()) {
                    Message message = AckList.remove(0); // Remove the first message for processing
                    System.out.println("Processing message: " + message);
                    if(message.getOperation() == OPERATION.ACK){

                        // UUID id = message.getPayload().getId();
                        // ACK_counts.putIfAbsent(id, 0);
                        // int currentCount = ACK_counts.get(id);
                        // currentCount++;
                        // ACK_counts.put(id, currentCount);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * Get all received ACK messages
     */
    public synchronized List<Message> getAckMessages() {
        return new ArrayList<>(AckList);
    }
    /**
     * Method to retrieve ACK counts without accidental modification
     * @return Map of the ACK counts per document
     */
    public synchronized Map<UUID, Integer> getAckCounts() {
        return new HashMap<>(ACK_counts); // Return a copy to avoid concurrent modification
    }

    /**
     * Method to increment the number of active nodes
     *  */
    public void incrementActiveNodes() {
        int i = numOfActiveNodes.incrementAndGet();
        System.out.println("Active nodes incremented. Total: " + i);
    }

    /**  
     * Method to decrement the number of active nodes
     * */
    public void decrementActiveNodes() {
        int i = numOfActiveNodes.decrementAndGet();
        System.out.println("Active nodes decremented. Total: " + i);
    }
    /**
     * Method to get the number of active nodes
     */
    public int getActiveNodeCount() {
        return numOfActiveNodes.get();
    }



/*
    public synchronized void sendAck(ACKMessage ack) throws RemoteException {
        UUID documentoId = ack.getDocumentoId();
        UUID elementoId = ack.getElementoId();
        ackCounts.putIfAbsent(documentoId, 0);
        int currentCount = ackCounts.get(documentoId);
        currentCount++;
        ackCounts.put(documentoId, currentCount);

        System.out.println("Recebi ACK do elemento: " + elementoId + " para o documento: " + documentoId);


        if (currentCount == gestorElementos.getExpectedAckCount()) {
            System.out.println("Todos os elementos receberam o documento: " + documentoId);
            processDocument(documentoId);
        }
    }

    public void processDocument(UUID documentoId) {
        messageList.commitDocumento(documentoId);
        sendTransmitter.sendDocumentAckMessage(documentoId);
        ackCounts.remove(documentoId);
    }

    */
}
