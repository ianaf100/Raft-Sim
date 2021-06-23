package iaf29.a2.raft.server;

// Additional exception similar to ActorKilledException intended for RaftServer actors

public class ServerStalledException extends RuntimeException {
    
    public ServerStalledException(String msg) {
        super(msg);
    }
    public ServerStalledException(java.lang.String message, java.lang.Throwable cause){
        super(message, cause);
    }
}
