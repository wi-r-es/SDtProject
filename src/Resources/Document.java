package Resources;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a document with an ID, content, and version.
 */
public class Document implements Serializable {
    private final UUID id;
    private String content;
    private int version;

    /**
     * Constructs a new document with the specified content.
     * Generates a random UUID for the document ID and initializes the version to 0.
     *
     * @param content The content of the document.
     */
    public Document(String content) {

        this.id = UUID.randomUUID();
        this.content = content;
        this.version = 0;
    }
    /**
     * Constructs a new document with the specified content, ID, and version.
     * Used as a copy constructor for cloning documents.
     *
     * @param content The content of the document.
     * @param id      The ID of the document.
     * @param version The version of the document.
     */
    public Document(String content, UUID id, int version){

        this.id = id;
        this.content = content;
        this.version = version;
    }
    /**
     * Constructs a new document from a Document String.
     * @param str The String version of the document.
     */
    public static Document fromString(String str) {
        // Expected format: {id='uuid', content='text', version='number'}
        Pattern pattern = Pattern.compile("\\{?id='(.*?)', content='(.*?)', version='(\\d+)'\\}?");
        Matcher matcher = pattern.matcher(str);
        
        if (matcher.find()) {
            String id = matcher.group(1);
            String content = matcher.group(2);
            int version = Integer.parseInt(matcher.group(3));
            return new Document(content, UUID.fromString(id), version);
        }
        throw new IllegalArgumentException("Invalid document string format: " + str);
    }
    

    // Getters and setters for document properties
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
    /**
     * Creates a clone of the specified document.
     *
     * @param doc The document to clone.
     * @return A new document instance with the same content, ID, and version as the original document.
     * @throws CloneNotSupportedException If cloning is not supported.
     */
    public static Document clone(Document doc) throws CloneNotSupportedException {
        return new Document(doc.getContent(), doc.getId(), doc.getVersion());
    }
    
    // Override methods for toString, equals, and hashCode
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