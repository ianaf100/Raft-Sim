package iaf29.a2.raft.statemachine;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


// Represents an arbitrary state machine which is a file.
// Input commands are strings which are appended to file when applied to the state machine.
// All responses (read/write) are the number of commands applied.

public class StateMachine {
    private String id;
    private FileWriter writer;
    private int count = 0;
    private int lastSerial;

    public StateMachine(String id) throws IOException {
        this.id = id;
        setupNewFile();
    }

    public int lastSerialNumber() {
        return lastSerial;
    }

    public int lastResponse() {
        return count;
    }

    public int applyInput(Command input) throws IOException {
        lastSerial = input.serialNumber;
        appendToFile(input.toString());
        count++;
        return count;
    }

    public int read() {
        return count;
    }

    private void appendToFile(String input) throws IOException {
        writer.write(input);
        writer.write("\n");
        writer.flush();
    }

    private void setupNewFile() throws IOException {
        File dir = new File("storage/statemachine");
        dir.mkdirs();
        try {
            this.writer = new FileWriter("storage/statemachine/" + id, false);
            this.writer.close(); // clear file from previous run if it exists
            this.writer = new FileWriter("storage/statemachine/" + id, false);
        } catch (IOException e) {
            System.err.println("File could not be created");
            throw e;
        }
    }

}
