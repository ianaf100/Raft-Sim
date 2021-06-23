package iaf29.a2.raft.log;

import java.util.LinkedList;


// A list of log entries representing a raft server's local log

// Uses ONE-BASED indexing (first index is 1) because raft logs uses one-based indexing

public class LocalLog {
    private LinkedList<LogEntry> log = new LinkedList<LogEntry>();

    public void appendEntry(LogEntry entry) {
        log.add(entry);
    }

    public LogEntry getEntry(int index) {
        index -= 1;
        return log.get(index);
    }

    public void setEntry(int index, LogEntry entry) {
        index -= 1;
        log.set(index, entry);
    }

    public LogEntry removeEntry(int index) {
        index -= 1;
        return log.remove(index);
    }

    public void removeTail(int startingIndex) {
        startingIndex -= 1;
        log.subList(startingIndex, log.size()).clear();
    }

    public int lastIndex() {
        return log.size();
    }

    public int lastTerm() {
        if (log.isEmpty()) {
            return 0;
        }
        return log.getLast().term();
    }

    public int lastSerial() {
        if (log.isEmpty()) {
            return -1;
        }
        return log.getLast().command().serialNumber;
    }

    public LogEntry last() {
        if (size() == 0) {
            return null;
        }
        return getEntry(size());
    }

    public int size() {
        return log.size();
    }

    @Override
    public String toString() {
        String output = "[";
        if (log.size() > 10) {
            output += "...,";
        }
        for (int i = Math.max(0, log.size()-10); i < log.size(); i++) {
            output += log.get(i).term();
            if (i != log.size() - 1) {
                output +=  ",";
            }
        }
        return output + "]";
    }
}
