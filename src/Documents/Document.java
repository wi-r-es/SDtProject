package Documents;

import java.util.UUID;

public class Document {
    private final String id;
    private String content;

    public Document(String content) {
        this.id = UUID.randomUUID().toString();
        this.content = content;
    }

    public String getId() {
        return id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
