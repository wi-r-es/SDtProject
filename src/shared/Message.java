package shared;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import Resources.Document;

public class Message implements Serializable{
    private final AtomicInteger message_id = new AtomicInteger(0);
    private final Document doc;
    private final String operation;
    private final long timestamp;

    public Message(Document document, String op){
        this.doc = document;
        this.operation=op;
        this.timestamp = System.currentTimeMillis();
        message_id.getAndIncrement();
    }

    public String getOperation(){
        return operation;
    }

    public Document getDocument(){
        return doc;
    }

    public int getMessageId(){
        return message_id.get();
    }
    
    public long getTimestamp(){
        return this.timestamp;
    }

    @Override
    public String toString(){
        return "MessageID:" + message_id.get() + ";" + doc.toString() + ";" + operation + ";" + timestamp + "END";
    }
}
