import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Scanner;

public class Client {

    private Socket socket;
    private BufferedReader bufferedReader;
    private BufferedWriter bufferedWriter;
    private String username;

    public Client(Socket socket, String username) {
        try {
            this.socket = socket;
            this.bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            this.username = username;
        } catch (IOException e) {
            closeEverything(socket, bufferedReader, bufferedWriter);
        }
    }


    public void sendMessage(){
        try {
            bufferedWriter.write(username);
            bufferedWriter.newLine();
            bufferedWriter.flush();

            Scanner scanner = new Scanner(System.in);
            while (socket.isConnected()){
                String messageToSend = scanner.nextLine();
                bufferedWriter.write(username + ": " + messageToSend);
                bufferedWriter.newLine();
                bufferedWriter.flush();
                if (messageToSend.equalsIgnoreCase("file")){
                    System.out.println("Please provide a valid path: ");
                    String filePath = scanner.nextLine();
                    MultimediaFile fileToSend = new MultimediaFile(filePath);
                    sendFile(fileToSend);
                }
            }
        } catch (IOException e) {
            closeEverything(socket, bufferedReader, bufferedWriter);
        }
    }

    public void sendFile(MultimediaFile file){
        try {
            while (socket.isConnected()){
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

                String fileName = file.getFileName();
                byte[] fileNameBytes = fileName.getBytes();
                dos.writeInt(fileNameBytes.length);
                dos.write(fileNameBytes);

                List<byte[]> chunks = file.splitInChunks();
                dos.writeInt(file.getNumberOfChunks());
                for (byte[] aByte : chunks) {
                    dos.write(aByte);
                }
                dos.flush();
            }
        } catch (IOException e) {
            closeEverything(socket, bufferedReader, bufferedWriter);
        }
    }

    public void listenForMessage(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                String msgFromGroupChat;

                while(socket.isConnected()){
                   try{
                       msgFromGroupChat = bufferedReader.readLine();
                       System.out.println(msgFromGroupChat);
                       if (msgFromGroupChat.startsWith("Heads up!")){
                           receiveFile();
                       }
                   } catch (IOException e){
                        closeEverything(socket, bufferedReader, bufferedWriter);
                   }
                }
            }
        }).start();
    }

    public void receiveFile(){
        File downloadedFile = new File("C:\\Users\\kosta\\Desktop\\new_download.txt");
        try{
            DataInputStream dis = new DataInputStream(socket.getInputStream());

            int size = dis.readInt();
            byte[] fileData = new byte[size];
            int readBytes = dis.read(fileData,0, size);
            System.out.println("READ BYTES:" + readBytes);
            FileOutputStream os = new FileOutputStream(downloadedFile);
            os.write(fileData);
            os.close();
        } catch (IOException e){
            System.out.println("Could not get file!");
        }
    }

    public void closeEverything(Socket socket, BufferedReader bufferedReader, BufferedWriter bufferedWriter){
        try {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
            if (bufferedWriter != null) {
                bufferedWriter.close();
            }
            if (socket != null) {
                socket.close();
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {

        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter your username for the group chat: ");
        String username = scanner.nextLine();
        Socket socket = new Socket("localhost", 1234);
        Client client = new Client(socket, username);
        client.listenForMessage();
        client.sendMessage();
    }
}
