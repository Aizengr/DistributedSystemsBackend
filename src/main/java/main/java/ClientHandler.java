package main.java;

import com.google.common.base.Objects;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import java.io.*;
import java.net.Socket;
import java.util.*;

public class ClientHandler implements Runnable,Serializable {

    public static ArrayList<ClientHandler> clientHandlers = new ArrayList<>(); //all connections
    public static ArrayList<ClientHandler> connectedPublishers = new ArrayList<>(); //connected publishers
    public static ArrayList<ClientHandler> connectedConsumers = new ArrayList<>(); //connected consumers

    public static Multimap<Profile,String> knownPublishers = ArrayListMultimap.create(); //known publishers for each topic
    public static Multimap<Profile,String> registeredConsumers = ArrayListMultimap.create(); //known consumers for each topic
    public static Multimap<String,Value> messagesMap = LinkedListMultimap.create(); //conversation history

    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private String username;


    public ClientHandler(Socket socket){
        try {
            this.socket = socket;
            this.out = new ObjectOutputStream(socket.getOutputStream());
            this.in = new ObjectInputStream(socket.getInputStream());
            clientHandlers.add(this); //keeping all connections
            Value initMessage = (Value)in.readObject(); //reading the initial connection message on constructor checking component ID as well as username
            if (initMessage.getRequestType().equalsIgnoreCase("Publisher")) {
                connectedPublishers.add(this); //keeping only alive publishers
            }
            else if (initMessage.getRequestType().equalsIgnoreCase("Consumer")){
                connectedConsumers.add(this); //keeping only alive consumers
            }
            this.username = initMessage.getUsername();
        } catch (IOException | ClassNotFoundException e) {
            closeEverything(socket, out, in);
        }
    }
    public void run() {
        Object streamObject = readStream();
        System.out.println(streamObject);
        int correctPort = -1;
        if (streamObject instanceof String topic) {
            while (correctPort <= 0) { //while provided topic does not exist, we continuously ask for a valid one from the component
                correctPort = Broker.searchBroker(topic);
                sendCorrectBroker(correctPort);
                if (correctPort <= 0) {
                    topic = (String) readStream();
                }
            }
        }
        if (correctPort == this.socket.getLocalPort()) { //if we are on the correct broker
            while (!socket.isClosed()) {
                Value value = (Value) readStream();
                System.out.println(value);
                if (value != null) {
                    if (value.getRequestType().equalsIgnoreCase("Publisher")) {
                        checkPublisher(value.getProfile(), value.getTopic());
                        if (!value.isFile()) {
                            messagesMap.put(value.getTopic(), value); //adding to history
                            broadcastMessage(value.getTopic(), value);//live message broadcasting to all connected consumers
                        } else {
                            List<Value> chunkList = new ArrayList<>();
                            while (value.getRemainingChunks() >= 0) { //while having remaining chunks for a file, read them
                                try {
                                    messagesMap.put(value.getTopic(), value); //adding all chunks to history
                                    chunkList.add(value);
                                    if (value.getRemainingChunks() == 0) {break;}
                                    value = (Value) in.readObject();
                                    System.out.println(value.getTopic());
                                    System.out.println(value);
                                } catch (IOException | ClassNotFoundException e) {
                                    System.out.println(e.getMessage());
                                }
                            }
                            broadcastFile(value.getTopic(), chunkList); // live file sharing to all connected consumers
                        }
                    } else if (value.getRequestType().equalsIgnoreCase("Consumer") && value.getMessage().equalsIgnoreCase("datareq")) { //initial case
                        checkConsumer(value.getProfile(), value.getTopic());
                        pull(value.getTopic()); //pulling history for the connected consumer with the datareq request
                    }
                }
            }
        } else {
            checkRemoveConsumer(correctPort); //check and remove consumer from alive connections
            checkRemovePublisher(correctPort); //in case of redirecting to another broker
        }
    }

    private void checkRemoveConsumer(int port){ //removes connected consumer in case of redirection to another broker
        if (connectedConsumers.contains(this)){
            System.out.println("SYSTEM: Redirecting consumer of: " + this.getUsername()
                    + " to Broker on port: " + port);
            connectedConsumers.remove(this);
        }
    }

    private void checkRemovePublisher(int port){ //removes connected publisher in case of redirection to another broker
        if (connectedPublishers.contains(this)){
            System.out.println("SYSTEM: Redirecting publisher of: " + this.getUsername()
                    + " to Broker on port: " + port);
            connectedConsumers.remove(this);
        }
    }


    private synchronized void sendCorrectBroker(int port){
        try {
            out.writeObject(port); //sending correct broker port to UserNode
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void broadcastFile(String topic, List<Value> chunkList){ //broadcasting all chunks on the chunk list passed to all users connected
        for (ClientHandler consumer : connectedConsumers) { //except the one sending them
            if (!consumer.getUsername().equalsIgnoreCase(this.username)) {
                System.out.println("File sharing to topic: " + topic.toUpperCase() +
                        " from: " + this.username + " to: " + consumer.getUsername());
                try {
                    for (Value value : chunkList) {
                        value.setRequestType("liveFile");
                        consumer.out.writeObject(value);
                        consumer.out.flush();
                    }
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    private void broadcastMessage(String topic, Value value){ //same as above for messages
        value.setRequestType("liveMessage");
        for (ClientHandler consumer : connectedConsumers){
            if (!consumer.getUsername().equalsIgnoreCase(this.username)){
                System.out.println("Broadcasting to topic: " + topic.toUpperCase() +
                        "for: " + this.username + " and value: " + value);
                try {
                    consumer.out.writeObject(value);
                    consumer.out.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private synchronized void pull(String topic){ //main pull function
        int count = checkValueCount(topic); //retrieving messages and files from history data structure and sending them
        try {
            out.writeObject(count);
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (Map.Entry<String,Value> entry : messagesMap.entries()){
            if (entry.getKey().equalsIgnoreCase(topic)){
                try {
                    System.out.println("SYSTEM: Pulling: "  + entry.getValue());
                    out.writeObject(entry.getValue());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private synchronized int checkValueCount(String topic){ //checks how many messages we have for the specific topic
        int count = 0;
        for (Map.Entry<String,Value> entry : messagesMap.entries()){
            if (entry.getKey().equalsIgnoreCase(topic)){
                count++;
            }
        }
        return count;
    }

    public void checkConsumer(Profile profile, String topic){ //checks if consumer is known for the topic and adds them
        if (!(registeredConsumers.containsEntry(profile,topic))){
            System.out.println("SYSTEM: New consumer registered to topic: " + topic
                    + " with username: " + profile.getUsername());
            registeredConsumers.put(profile, topic);
        }
    }
    public void checkPublisher(Profile profile, String topic){ //checks if publisher is known for the topic and adds them
        if (!(knownPublishers.containsEntry(profile, topic))){
            System.out.println("SYSTEM: New publisher added to known Publishers for topic: " + topic
                    + " with username: " +profile.getUsername());
            knownPublishers.put(profile, topic);
        }
    }

    public synchronized Object readStream(){ //main reading object method
        try {
            return in.readObject();
        } catch (ClassNotFoundException | IOException e){
            closeEverything(socket, out, in);
            System.out.println(e.getMessage());
        }
        return null;
    }

    public String getUsername(){
        return this.username;
    }

    public void removeClientHandler(){ //disconnects client (both consumer and publisher)
        clientHandlers.remove(this);
        connectedPublishers.remove(this);
        connectedConsumers.remove(this);
        System.out.println("SYSTEM: A component has disconnected!");
    }

    public void closeEverything(Socket socket, ObjectOutputStream out, ObjectInputStream in){
        removeClientHandler(); //removes client and closes everything
        try {
            if (out != null) {
                out.close();
            }
            if (in != null) {
                in.close();
            }
            if (socket != null) {
                socket.close();
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }
}
