import java.io.IOException;
import java.net.*;
import java.util.HashMap;

import static java.lang.Thread.sleep;

public class SendTransmitter extends Thread{
    //static private HashMap<Integer, String> messagesList = new HashMap<>();


    private DatagramSocket socket;
    private InetAddress group;
    private byte[] buf;
    protected int port = 2323;

    @Override
    public void run() {
        try {

            while (true) {
                sendMessages(Element.getMessagesLists());
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void sendMessages(HashMap<Integer,String> messagesList) throws IOException {
        socket = new DatagramSocket();
        group = InetAddress.getByName("230.23.23.23");

        for(String message : messagesList.values()){
            buf = message.getBytes();

            DatagramPacket packet = new DatagramPacket(buf, buf.length, group, port);
            System.out.println(new String (packet.getData()) );
            socket.send(packet);
        }
        socket.close();
    }

}
