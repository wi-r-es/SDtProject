package Exception;

public class SyncFailureException extends Exception {
    public SyncFailureException(String message) {
        super(message);
    }

    public SyncFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}
