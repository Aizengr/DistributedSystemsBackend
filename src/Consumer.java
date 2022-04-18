import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;

public class Consumer extends UserNode implements Runnable, Serializable {



    public Consumer(Profile profile){
        super(profile);
        if (!socket.isConnected()){
            connect(socket);
        }
        aliveConsumerConnections.add(this);
    }

    public Consumer(Socket socket, Profile profile){
        super(socket, profile);
        connect(socket);
        aliveConsumerConnections.add(this);
    }

    @Override
    public void run() {

        System.out.println("Consumer established connection with Broker on port: " + this.socket.getPort());
        /*
        Value brokerList = getBrokerList();
        if (brokerList != null){

        }
    }

    public synchronized Value getBrokerList(){
        Value value = new Value("Brokerlist.", "Consumer");
        try {
            objectOutputStream.writeObject(value);
            objectOutputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Value answer = null;
        try {
            if (objectInputStream.readObject() instanceof Value){
                answer = (Value)objectInputStream.readObject();
            }
        }catch (IOException | ClassNotFoundException e){
            e.printStackTrace();
        }
        return answer;
    }

    public synchronized void updateBrokerAndTopicInformation(){

    }
    */

    }
}
