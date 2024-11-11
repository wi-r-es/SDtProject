package Documents;

public class Operation {
    private final String documentId;
    private final String operation;

    public Operation(String documentId, String op) {
        this.documentId = documentId;
        this.operation = op;
    }

    public String getDocumentId() {
        return documentId;
    }

    public String getContent() {
        return operation;
    }
}
