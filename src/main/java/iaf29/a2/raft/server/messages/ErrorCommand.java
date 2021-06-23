package iaf29.a2.raft.server.messages;

public class ErrorCommand {

    public final RuntimeException exception;
    public boolean terminate;
    
    public ErrorCommand(RuntimeException exception) {
        this(exception, false);
    }

    public ErrorCommand(RuntimeException exception, boolean terminate) {
        this.exception = exception;
        this.terminate = terminate;
    }
}
