package iaf29.a2.raft.server;

// Additional exception similar to ActorKilledException intended for RaftServer actors

public class ServerStoppedException extends RuntimeException {
    
    public ServerStoppedException(String msg) {
        super(msg);
    }
    public ServerStoppedException(java.lang.String message, java.lang.Throwable cause){
        super(message, cause);
    }
}
