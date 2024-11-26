package Nodes.Raft;

public class LogEntry {
    private final int term;        // The term when this entry was received by the leader
    private final int index;       // The index of this entry in the log
    private final String command;  // The command to be executed by the state machine

    public LogEntry(int term, int index, String command) {
        this.term = term;
        this.index = index;
        this.command = command;
    }

    // Getters
    public int getTerm() {
        return term;
    }

    public int getIndex() {
        return index;
    }

    public String getCommand() {
        return command;
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "term=" + term +
                ", index=" + index +
                ", command='" + command + '\'' +
                '}';
    }
}


