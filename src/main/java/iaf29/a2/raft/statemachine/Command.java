package iaf29.a2.raft.statemachine;

// State machine commands are strings
public class Command {
    
    public final int serialNumber;
    public final String command;

    public Command(int serialNumber, String command) {
        this.serialNumber = serialNumber;
        this.command = command;
    }

    @Override
    public String toString() {
        return command;
    }
    
}
