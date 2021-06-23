package iaf29.a2.raft.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import akka.actor.ActorRef;
import iaf29.a2.raft.log.LocalLog;
import iaf29.a2.raft.log.LogEntry;
import iaf29.a2.raft.server.messages.AppendEntriesRPC;
import iaf29.a2.raft.statemachine.Command;


// StateModule maintains state for a Raft server and handles storing/loading persistent state 
//   to files. Maintains 2 files: 
//    - a state file (storage/server-x_state) containing currentTerm and votedFor, 
//    - a log file (log.server-x_log) containing the log

public class StateModule {
    ActorRef self;
    String id;
    FileWriter logWriter;
    int logFlushedIndex;  // index of last entry to be flushed to disk
    
    // Raft state
    public int currentTerm;
    public ActorRef votedFor;
    public LocalLog localLog;
    public List<ActorRef> otherServers;
    public ActorRef lastLeader = null;
    public int commitIndex = 0;
    public int lastApplied = 0; 
    
    // Leader state
    public Map<ActorRef, Integer> nextIndex = new HashMap<ActorRef, Integer>();
    public Map<ActorRef, Integer> matchIndex = new HashMap<ActorRef, Integer>();
    public Map<ActorRef, AppendEntriesRPC> activeRequests = new HashMap<ActorRef, AppendEntriesRPC>();
    public int nextRequestId = 0;

    // Candidate state
    Set<ActorRef> yesVotes = new HashSet<ActorRef>();
    Set<ActorRef> noVotes = new HashSet<ActorRef>();


    public StateModule(ActorRef self, List<ActorRef> servers) throws IOException {
        this(self, servers, false);
    }

    public StateModule(ActorRef self, List<ActorRef> servers, boolean restoreFromDisk) throws IOException {
        this.self = self;
        this.id = getActorName(self);
        otherServers = new ArrayList<ActorRef>();
        for (ActorRef server : servers) {
            if (server != self) {
                otherServers.add(server);
            }
        }
        if (restoreFromDisk) {
            restorePersistentState();
        } else {
            currentTerm = 0;
            votedFor = null;
            logFlushedIndex = 0; 
            localLog = new LocalLog();
            setupNewFiles();
        }
    }

    StateModule() {}  // testing

    public String getActorName(ActorRef actor) {
        String[] split = actor.path().toString().split("/");
        return split[split.length - 1];
    }

    public int getNextIndex(ActorRef server) {
        return nextIndex.get(server);
    }

    public void setNextIndex(ActorRef server, int newIndex) {
        nextIndex.put(server, newIndex);
    }

    public int getMatchIndex(ActorRef server) {
        return matchIndex.get(server);
    }

    public void setMatchIndex(ActorRef server, int newIndex) {
        matchIndex.put(server, newIndex);
    }

    public String id() {
        return id;
    }

    private void setupNewFiles() throws IOException {
        File dir = new File("storage");
        dir.mkdir();
        openNewLogFile();
        writeStateFile();
    }

    private void openNewLogFile() throws IOException {
        try {
            logWriter = new FileWriter("storage/" + id + "_log", false);
            logWriter.close(); // clear file from previous run if it exists
            logWriter = new FileWriter("storage/" + id + "_log", false);
        } catch (IOException e) {
            System.err.println("File could not be created");
            throw e;
        }
    }

    // state file is 2 lines, first = currentTerm, second = votedFor (actor id)
    private void writeStateFile() throws IOException {
        try {
            FileWriter writer = new FileWriter("storage/" + id + "_state", false);
            writer.write(currentTerm + "\n");
            if (votedFor == null) 
                writer.write("null\n");
            else
                writer.write(getActorName(votedFor) + "\n");
            writer.close();
        } catch (IOException e) {
            System.err.println("File could not be created");
            throw e;
        }
    }

    private void restorePersistentState() throws IOException {
        restoreLog();
        restoreState();
    }

    private void restoreLog() throws IOException {
        String filepath = "storage/" + id + "_log"; 
        restoreLog(filepath);
    }


    void restoreLog(String filepath) throws IOException {
        localLog = new LocalLog();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filepath));
            String line = reader.readLine();
            while (line != null) {
                String[] splitLine = line.split(":");
                int term = Integer.parseInt(splitLine[0]);
                Command command = new Command(0, splitLine[1]);
                localLog.appendEntry(new LogEntry(command, term));
                line = reader.readLine();
            }
            reader.close();
            logWriter = new FileWriter("storage/" + id + "_log", true);  // append to old log file
            
        } catch (IOException e) {
            openNewLogFile();
        }
        logFlushedIndex = localLog.size();
    }


    private void restoreState() throws IOException {
        try {
            BufferedReader reader = new BufferedReader(new FileReader("storage/" + id + "_state"));
            String term = reader.readLine();
            if (term == null) {
                currentTerm = 0;
            } else {
                currentTerm = Integer.parseInt(term);
            }
            String actorId = reader.readLine();
            if (actorId == null || actorId.equals("null")) {
                votedFor = null;
            } else if (actorId.equals(id)) {
                votedFor = self;
            } else {
                for (ActorRef server : otherServers) {
                    if (getActorName(server).equals(actorId)) {
                        votedFor = server;
                    }
                }
            }
            reader.close();
        } catch (IOException e) {
            currentTerm = 0;
            votedFor = null;
            writeStateFile();
        }
    }

    public void flushToDisk() throws IOException {
        writeStateFile();

        // log file has 1 line per entry, formatted: termNumber:command
        for (int i = logFlushedIndex + 1; i < localLog.size() + 1; i++) {
            LogEntry newEntry = localLog.getEntry(i);
            logWriter.write(newEntry.term() + ":" + newEntry.command().toString());
            logWriter.write("\n");
        }
        logWriter.flush();
        logFlushedIndex = localLog.size();
    }

    public void setLeaderState() {
        lastLeader = self;
        yesVotes.clear();
        noVotes.clear();
        for (ActorRef server : otherServers) {
            setNextIndex(server, localLog.lastIndex() + 1);
            setMatchIndex(server, 0);
        }
    }

    // reset leader (and candidate) state
    public void clearLeaderState() {
        votedFor = null;
        yesVotes.clear();
        noVotes.clear();
        nextIndex.clear();
        matchIndex.clear();
        activeRequests.clear();
    }

    // leader method - checks and potentially update commit index
    public boolean checkCommitIndex(int n) {
        if (n > commitIndex && localLog.getEntry(n).term() == currentTerm) {
            int majority = 1;
            for (ActorRef server : otherServers) {
                int matchIndex = getMatchIndex(server);
                if (matchIndex >= n) {
                    majority++;
                }
            }
            if (majority >= ((otherServers.size() + 1) / 2) + 1) {
                commitIndex = n;
                return true;
            }
        }
        return false;
    }

}
