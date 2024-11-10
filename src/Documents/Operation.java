package Documents;

public class Operation {
    private final String documentId;
    private final String content;

    public Operation(String documentId, String content) {
        this.documentId = documentId;
        this.content = content;
    }

    public String getDocumentId() {
        return documentId;
    }

    public String getContent() {
        return content;
    }
}
