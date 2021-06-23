package iaf29.a2.raft.log;

import iaf29.a2.raft.statemachine.Command;

public class LogEntry {
    
    final private Command command;
    final private int term;

    public LogEntry(Command command, int term) {
        this.command = command;
        this.term = term;
    }

    public Command command() {
        return command;
    }

    public int term() {
        return term;
    }

    @Override
    public String toString() {
        return command.toString();
    }
}
