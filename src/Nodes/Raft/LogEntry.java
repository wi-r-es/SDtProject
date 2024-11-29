package Nodes.Raft;

/**
 * Represents a log entry in the Raft consensus algorithm.
 */
public class LogEntry {
    private final int term;        // The term when this entry was received by the leader
    private final int index;       // The index of this entry in the log
    private final String command;  // The command to be executed by the state machine

    /**
     * Constructs an instance of LogEntry.
     *
     * @param term    The term when this entry was received by the leader.
     * @param index   The index of this entry in the log.
     * @param command The command to be executed by the state machine.
     */
    public LogEntry(int term, int index, String command) {
        this.term = term;
        this.index = index;
        this.command = command;
    }

    // Getters for the fields
    public int getTerm() {
        return term;
    }

    public int getIndex() {
        return index;
    }

    public String getCommand() {
        return command;
    }

    /**
     * Returns a string representation of the LogEntry.
     *
     * @return A string representation of the LogEntry.
     */
    @Override
    public String toString() {
        return "LogEntry{" +
                "term=" + term +
                ", index=" + index +
                ", command='" + command + '\'' +
                '}';
    }
}


