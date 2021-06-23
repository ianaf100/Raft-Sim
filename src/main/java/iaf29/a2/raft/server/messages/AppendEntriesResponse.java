package iaf29.a2.raft.server.messages;

public class AppendEntriesResponse {
    public final int term;
    public final boolean success;
    public final int requestId;

    public AppendEntriesResponse(int term, boolean success, int requestId) {
        this.term = term; 
        this.success = success;
        this.requestId = requestId;
    }
}
