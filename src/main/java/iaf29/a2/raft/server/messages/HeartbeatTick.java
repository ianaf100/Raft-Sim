package iaf29.a2.raft.server.messages;

import akka.actor.ActorRef;

public class HeartbeatTick {
    public final ActorRef receiver;

    public HeartbeatTick(ActorRef receiver) {
        this.receiver = receiver;
    }
}
