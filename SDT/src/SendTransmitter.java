import java.io.IOException;
import java.net.*;
import java.util.HashMap;

import static java.lang.Thread.sleep;

public class SendTransmitter {
    static private HashMap<Integer, String> messagesList = new HashMap<>();
    private DatagramSocket socket;
    private InetAddress group;
    private byte[] buf;
    protected int port = 2323;

    public void run() throws InterruptedException, IOException {
        while (true){
            sendMessages(messagesList);
            sleep(5000);
        }
    }

    private void sendMessages(HashMap<Integer,String> messagesList) throws IOException {
        socket = new DatagramSocket();
        group = InetAddress.getByName("230.23.23.23");
        for(String message : messagesList.values()){
            buf = message.getBytes();

            DatagramPacket packet = new DatagramPacket(buf, buf.length, group, port);
            socket.send(packet);
        }
        socket.close();
    }

}
