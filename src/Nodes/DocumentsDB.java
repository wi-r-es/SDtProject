package Nodes;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import Resources.Document;
/**
* The DocumentsDB class represents a database of documents.
* It uses a ConcurrentHashMap to store the documents, allowing concurrent access.
* It also provides methods for updating, adding, and removing documents, as well as managing temporary changes.
*/
public class DocumentsDB {
    private ConcurrentHashMap<UUID, Document> documentMap; 
    private ConcurrentHashMap<UUID, Document> tempMap;
    private boolean locked;

    private final Object lockObject = new Object();
    /**
    * Constructor for the DocumentsDB class.
    * Initializes the documentMap and sets tempMap to null.
    */
    public DocumentsDB(){
        documentMap = new ConcurrentHashMap<>();
        tempMap = null;
    }
    

        /*
██████   ██████   ██████      ██████  ██████  ███████ 
██   ██ ██    ██ ██          ██    ██ ██   ██ ██      
██   ██ ██    ██ ██          ██    ██ ██████  ███████ 
██   ██ ██    ██ ██          ██    ██ ██           ██ 
██████   ██████   ██████      ██████  ██      ███████ 
                                                      
     */
    /**
    * Updates or adds a document to the database.
    *
    * @param newDoc The document to update or add.
    * @return True if the document was updated or added, false if the document is already up-to-date.
    */
    public synchronized boolean updateOrAddDocument(Document newDoc) {
        Document existingDoc = documentMap.get(newDoc.getId());
        
        if (existingDoc == null || existingDoc.getVersion() < newDoc.getVersion()) {
            documentMap.put(newDoc.getId(), newDoc);
            System.out.println(existingDoc == null ? "Document added: " + newDoc : "Document updated: " + newDoc);
            return true;
        } else {
            System.out.println("Document already up-to-date: " + existingDoc);
            return false;
        }
    }
    /**
    * Removes a document from the database.
    *
    * @param document The document to remove.
    * @return True if the document was removed, false otherwise.
    */
    public synchronized boolean removeDocument(Document document){
        return documentMap.remove(document.getId(), document);
    }

    /**
    * Creates a temporary map of documents. (to revert syncing operations)
    * The temporary map is a copy of the current documentMap.
    */
    public void createTempMap() {
        tempMap = new ConcurrentHashMap<>(documentMap);
    }
    /**
    * Commits the changes made to the temporary map.
    * The temporary map is set to null after committing.
    */
    public void commitChanges() {
        //documentMap = tempMap;
        tempMap = null;
    }
    /**
    * Reverts the changes made to the temporary map.
    * The documentMap is restored to the state of the temporary map.
    * The temporary map is set to null after reverting.
    */
    public void revertChanges() {
        documentMap = tempMap;
        tempMap = null;
    }
    /**
    * Returns the temporary map of documents.
    *
    * @return The temporary map of documents.
    */
    protected ConcurrentHashMap<UUID, Document> getTempDocumentsMap() {
        return tempMap;
    }
    /**
    * Returns the current map of documents.
    *
    * @return The current map of documents.
    */
    protected ConcurrentHashMap<UUID, Document> getDocumentsMap() {
        return documentMap;
    }
    /**
    * Returns a copy of the current map of documents.
    *
    * @return A copy of the current map of documents.
    */
    public ConcurrentHashMap<UUID, Document> getDocuments() {
        return new ConcurrentHashMap<UUID, Document>(documentMap);
    }


    /**
    * Checks if a temporary map exists.
    *
    * @return True if a temporary map exists, false otherwise.
    */
    public boolean tempMapExists(){
        if(tempMap!=null){
            return true;
        }
        return false;
    }


    /**
    * Acquires a lock on the database.
    * If the database is already locked, the method waits until the lock is released.
    */
    public void lock() {
        synchronized (lockObject) {
            while (locked) {
                try {
                    lockObject.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            locked = true;
        }
    }
    /**
    * Releases the lock on the database.
    * Notifies all waiting threads that the lock has been released.
    */
    public void unlock() {
        synchronized (lockObject) {
            locked = false;
            lockObject.notifyAll();
        }
    }
    /**
    * Checks if the database is currently locked.
    *
    * @return True if the database is locked, false otherwise.
    */
    protected boolean isLocked(){
        synchronized (lockObject) {
            return locked;
        }
    }
}
