import java.util.HashMap;
import java.util.Queue;

public class Profile {

    private String username;
    private HashMap<String,MultimediaFile> userMultimediaFileMap;
    private Queue<MultimediaFile> pendingUpload;
    private HashMap<String,String> userSubscribedConversations;

    public Profile(String username){
        this.username = username;
    }

    public void addFileToProfile(String fileName, MultimediaFile file){
        userMultimediaFileMap.put(fileName,file);
    }

    public void addFileToUploadQueue(MultimediaFile file){
        pendingUpload.add(file);
    }

    public MultimediaFile getFileFromUploadQueue(){
        return pendingUpload.poll();
    }

    public boolean checkSub(String topic){
        return userSubscribedConversations.containsValue(topic);
    }

    public void sub(String id, String topic){
        userSubscribedConversations.put(id,topic);
    }

    public void removeFile(String name){
        userMultimediaFileMap.remove(name);
    }

    public void unSub(String conversationName){
        userSubscribedConversations.remove(conversationName);
    }

    public String getUsername(){
        return this.username;
    }

    public void setUserName(String username){
        this.username = username;
    }

    public int checkUploadQueueCount(){
        return pendingUpload.size();
    }

}