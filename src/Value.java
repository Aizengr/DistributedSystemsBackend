import java.io.Serializable;
import java.util.Arrays;

public class Value implements Serializable {    //serializable object for all kinds of communication
                                                // with brokers as well as data passing

    private String message;
    private Profile profile;
    private String filename;
    private final String requestType;
    private byte[] chunk;
    private int remainingChunks;
    private final boolean fileSharing;

    public Value(String message, String requestType){
        this.message = message;
        this.requestType = requestType;
        this.fileSharing = false;
    }

    public Value(String message, Profile profile, String requestType) {
        this.message = message;
        this.profile = profile;
        this.requestType = requestType;
        this.fileSharing = false;
    }

    public Value(Value value){
        this.message = value.message;
        this.profile = value.profile;
        this.filename = value.filename;
        this.remainingChunks = value.remainingChunks;
        this.chunk = Arrays.copyOf(value.chunk,value.chunk.length);
        this.requestType = value.requestType;
        this.fileSharing = false;
    }

    public Value(String message, String chunkName, Profile profile, int remainingChunks, byte[] chunk, String requestType){
        this.message = message;
        this.chunk = Arrays.copyOf(chunk,chunk.length);
        this.profile = profile;
        this.remainingChunks = remainingChunks;
        this.filename = chunkName;
        this.requestType = requestType;
        this.fileSharing = true;
    }

    @Override //toString override for printing non data sharing attr of our custom object
    public String toString() {
        return "Value{" +
                "message='" + message + '\'' +
                ", fileName='" + filename + '\'' +
                ", profileName='" + profile.getUsername() + '\'' +
                ", number of remaining Chunks='" + remainingChunks + '\'' +
                ", request type: '" + requestType + '\'' +
                '}';
    }


    public String getMessage() {
        return message;
    }

    public boolean isFile(){
        return this.fileSharing;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getUsername() {
        return this.profile.getUsername();
    }

    public String getRequestType(){
        return this.requestType;
    }

    public int getRemainingChunks() {
        return remainingChunks;
    }

    public String getFilename(){
        return this.filename;
    }

    public void setChunk(byte[] chunk) {
        this.chunk = chunk;
    }

    public byte[] getChunk() {
        return chunk;
    }

    public Profile getProfile() {
        return this.profile;
    }
}
