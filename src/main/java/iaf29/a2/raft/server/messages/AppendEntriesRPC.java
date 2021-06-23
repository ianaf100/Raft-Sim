package iaf29.a2.raft.server.messages;

import akka.actor.ActorRef;
import iaf29.a2.raft.log.LogEntry;

public class AppendEntriesRPC {
    public final int term;
    public final ActorRef leaderId;
    public final int prevLogIndex;
    public final int prevLogTerm;
    public final LogEntry[] entries;
    public final int leaderCommit;
    public final int requestId;


    public AppendEntriesRPC(int term, 
            ActorRef leaderId, 
            int prevLogIndex, 
            int prevLogTerm, 
            LogEntry[] entries, 
            int leaderCommit,
            int requestId) 
    {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
        this.requestId = requestId;
    }
}







/* 

term :		leader’s term
leaderId :		so follower can redirect clients
prevLogIndex :		index of log entry immediately preceding new ones
prevLogTerm :		term of prevLogIndex entry
entries[] :		log entries to store (empty for heartbeat; may send more than one for efficiency)
leaderCommit :		leader’s commitIndex
*/