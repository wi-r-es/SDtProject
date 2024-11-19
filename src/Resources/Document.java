package Resources;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

public class Document implements Serializable {
    private final UUID id;
    private String content;
    private int version;


    public Document(String content) {

        this.id = UUID.randomUUID();
        this.content = content;
        this.version = 0;
    }
    //copy constructor for clone method
    public Document(String content, UUID id, int version){

        this.id = id;
        this.content = content;
        this.version = version;
    }

    public UUID getId() {
        return id;
    }
    
    public String getContent() {
        return content;
    }
    public void setContent(String content) {
        this.content = content;
        this.version += 1;
    }

    public int getVersion() {
        return version;
    }
    public void setVersion(int v) {
        this.version = v;
    }
    public void incVersion() {
        this.version +=1;
    }

    // SYNC DOCUMENTS
    public static Document clone(Document doc) throws CloneNotSupportedException {
        return new Document(doc.getContent(), doc.getId(), doc.getVersion());
    }

    @Override
    public String toString() {
        return "Document{id='" + id + "', content='" + content  +  "', version='" + version + "'}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Document document = (Document) o;
        return Objects.equals(id, document.id) && Objects.equals(content, document.content) && Objects.equals(version, document.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}