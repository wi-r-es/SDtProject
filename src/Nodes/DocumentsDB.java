package Nodes;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import Resources.Document;

public class DocumentsDB {
    private ConcurrentHashMap<UUID, Document> documentMap; 
    private ConcurrentHashMap<UUID, Document> tempMap;
    private boolean locked;

    private final Object lockObject = new Object();

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
    public synchronized boolean removeDocument(Document document){
        return documentMap.remove(document.getId(), document);
    }

    // Operations for temporary list
    public void createTempMap() {
        tempMap = new ConcurrentHashMap<>(documentMap);
    }

    public void commitChanges() {
        //documentMap = tempMap;
        tempMap = null;
    }

    public void revertChanges() {
        documentMap = tempMap;
        tempMap = null;
    }
    protected ConcurrentHashMap<UUID, Document> getTempDocumentsMap() {
        return tempMap;
    }
    protected ConcurrentHashMap<UUID, Document> getDocumentsMap() {
        return documentMap;
    }
    public ConcurrentHashMap<UUID, Document> getDocuments() {
        return new ConcurrentHashMap<UUID, Document>(documentMap);
    }
    // protected void clearTempMap(){
    //     tempMap.clear();
    //     locked = false;
    // }
    
    public boolean tempMapExists(){
        if(tempMap!=null){
            return true;
        }
        return false;
    }



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

    public void unlock() {
        synchronized (lockObject) {
            locked = false;
            lockObject.notifyAll();
        }
    }

    protected boolean isLocked(){
        synchronized (lockObject) {
            return locked;
        }
    }
}
