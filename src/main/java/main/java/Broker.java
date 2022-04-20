package main.java;
import java.io.*;
import java.math.BigInteger;
import java.net.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import static java.lang.Integer.parseInt;

public class Broker implements Serializable {

    private final int id;
    private final InetAddress address;

    private static HashMap<Integer,String> portsAndAddresses = new HashMap<>(); //ports and addresses
    private static HashMap<Integer,Integer> availableBrokers =  new HashMap<>(); //ids, ports
    private static HashMap<Integer,String> hashedTopics = new HashMap<>();//hash and topics
    private static HashMap<Integer,Integer> hashedTopicsToBrokers = new HashMap<>(); //topic hashes their corresponding Broker
    private static List<String> availableTopics = new ArrayList<>();

    private final ServerSocket serverSocket;

    public Broker(ServerSocket serverSocket, InetAddress address, int id){
        this.serverSocket = serverSocket;
        this.address = address;
        this.id = id;
        readConfig(System.getProperty("user.dir").concat("\\src\\main\\java\\main\\java\\config.txt"));
    }

    public void startBroker(){
        try {
            while (!serverSocket.isClosed()){
                Socket socket = serverSocket.accept();
                System.out.println("SYSTEM: A new component connected!");
                ClientHandler clientHandler = new ClientHandler(socket);
                Thread thread = new Thread(clientHandler);
                thread.start();
            }
        } catch (IOException e){
            closeServerSocket();
        }
    }


    public void closeServerSocket(){
        try {
            if (serverSocket != null){
                serverSocket.close();
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public String getBrokerAddress(){
        return this.address.toString();
    }

    public String getBrokerPort(){
        return Integer.toString(serverSocket.getLocalPort());
    }


    public int getBrokerID(){
        return this.id;
    }

    private void readConfig(String path){ //reading ports, hostnames and topics from config file
        File file = new File(path); //same method on both brokers and user node
        try {
            Scanner reader = new Scanner(file);
            reader.useDelimiter(",");
            String id, hostname, port;
            while(reader.hasNext()){
                id = reader.next();
                while(!id.equalsIgnoreCase("#")){
                    hostname = reader.next();
                    port = reader.next();
                    portsAndAddresses.put(parseInt(port),hostname);
                    availableBrokers.put(parseInt(id),parseInt(port));
                    id = reader.next();
                }
                availableTopics.add(id);
            }
        } catch (FileNotFoundException e){
            System.out.println(e.getMessage());
        }
    }

    public int getBrokerHash(){
        String brokerHash = getBrokerAddress() + getBrokerPort();
        return brokerHash.hashCode();
    }

    public static int getBrokerHash(String address, String port){
        String brokerHash = address + port;
        return brokerHash.hashCode();
    }

    private static void hashTopics(){
        for (String topic : availableTopics){
            //hashedTopics.put();
        }
    }

    public static String encryptThisString(String input){ //SHA-1 encryption method
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] messageDigest = md.digest(input.getBytes());
            BigInteger no = new BigInteger(1, messageDigest);
            String hashtext = no.toString(16);
            while (hashtext.length() < 32) {
                hashtext = "0" + hashtext;
            }
            return hashtext;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }



    public static void main(String[] args) throws IOException {

        ServerSocket serverSocket = new ServerSocket(4000);
        Broker broker = new Broker(serverSocket, InetAddress.getByName("localhost"), 2);
        System.out.println("SYSTEM: Broker_" + broker.getBrokerID()+" connected at: " + serverSocket + "with address: " +  broker.getBrokerAddress()
        + " and hashcode: " + broker.getBrokerHash());
        broker.startBroker();
    }
}
