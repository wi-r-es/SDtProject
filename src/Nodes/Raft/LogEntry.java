package Nodes.Raft;

import java.io.Serializable;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import Resources.Document;

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
        // Format: LogEntry{term=4, index=0, command='CREATE:Document{id='777a834a-8be4-45a3-9ef8-893bbac78aa6', content='This is a new document1', version='0'}'}
        try {
            Pattern pattern = Pattern.compile("LogEntry\\{term=(\\d+), index=(\\d+), command='(.*?)'\\}'\\}");
            Matcher matcher = pattern.matcher(str);
            
            if (matcher.find()) {

                // Print the entire match
                System.out.println("Full match: " + matcher.group(0));
                
                // Print each captured group by index
                for (int i = 1; i <= matcher.groupCount(); i++) {
                    System.out.println("Group " + i + ": " + matcher.group(i));
                }

                int term = Integer.parseInt(matcher.group(1));
                int index = Integer.parseInt(matcher.group(2));
                String command = matcher.group(3);
                return new LogEntry(term, index, command);
            }
            else{
                Pattern pattern2 = Pattern.compile("LogEntry\\{term=(\\d+), index=(\\d+), command='(.*?)");
                Matcher matcher2 = pattern2.matcher(str);
                if(matcher2.find()){
                    System.out.println("Full match2: " + matcher.group(0));
                    int term = Integer.parseInt(matcher.group(1));
                    int index = Integer.parseInt(matcher.group(2));
                    String command = matcher.group(3);
                    return new LogEntry(term, index, command);
                }
                System.out.println("No match found.");
                throw new IllegalArgumentException("Invalid log entry format: " + str );
            }

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
                ", command='" + command + "'}" ;
    }
}


