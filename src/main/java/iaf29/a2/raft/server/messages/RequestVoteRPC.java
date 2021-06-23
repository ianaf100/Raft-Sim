package iaf29.a2.raft.server.messages;

import akka.actor.ActorRef;

public class RequestVoteRPC {
    

    public final int term;
    public final ActorRef candidateId;
    public final int lastLogIndex;
    public final int lastLogTerm;

    public RequestVoteRPC(int term, ActorRef candidateId, int lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    // term currentTerm, for candidate to update itself
    // voteGranted true means candidate received vote

}
