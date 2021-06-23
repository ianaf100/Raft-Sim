package iaf29.a2.raft.server.messages;

import iaf29.a2.raft.statemachine.Command;

public class WriteRequest {
    public final int serialNumber;
    public final Command command;

    public WriteRequest(Command command) {
        this.serialNumber = command.serialNumber;
        this.command = command;
    }

    public WriteRequest(int serialNumber) {
        this.serialNumber = serialNumber;
        this.command = null;
    }
}
