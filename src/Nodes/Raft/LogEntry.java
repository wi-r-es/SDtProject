package Nodes.Raft;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a log entry in the Raft consensus algorithm.
 */
public class LogEntry implements Serializable{
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
    /**
     * Constructs an instance of LogEntry from a string.
     *
     * @param str    The String version of the LogEntry.
     */
    public static LogEntry fromString(String str) {
        // Format: Term=1,Index=2,Command='CREATE:Document{...}'
        try {
            Pattern pattern = Pattern.compile("Term=(\\d+),Index=(\\d+),Command='(.*?)'");
            Matcher matcher = pattern.matcher(str);
            
            if (matcher.find()) {
                int term = Integer.parseInt(matcher.group(1));
                int index = Integer.parseInt(matcher.group(2));
                String command = matcher.group(3);
                return new LogEntry(term, index, command);
            }
            throw new IllegalArgumentException("Invalid log entry format: " + str);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error parsing log entry: " + str, e);
        }
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


