import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class SendTransmitter extends Thread {
    private static final String MULTICAST_ADDRESS = "230.0.0.1";
    private static final int PORT = 4446;
    private Element element;

    public SendTransmitter(Element element) {
        this.element = element;
    }

    private void sendHeartbeat() {
        try (MulticastSocket socket = new MulticastSocket()) {
            String message = "heartbeat";
            byte[] buffer = message.getBytes();
            InetAddress group = InetAddress.getByName(MULTICAST_ADDRESS);
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, PORT);
            socket.send(packet);
            System.out.println("Sent heartbeat from leader node: " + element.getId());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        if (element.isLider()) {
            while (true) {
                sendHeartbeat();
                try {
                    Thread.sleep(5000); // Send heartbeat every 5 seconds
                } catch (InterruptedException e) {
                    System.out.println("Heartbeat transmission interrupted.");
                    break;
                }
            }
        }
    }
}



/*
import java.io.IOException;

import java.net.*;
import java.util.HashMap;

import static java.lang.Thread.sleep;

public class SendTransmitter extends Thread{
    //static private HashMap<Integer, String> messagesList = new HashMap<>();
    private static final String MULTICAST_ADDRESS = "230.23.23.24";

    //private DatagramSocket socket;
    private MulticastSocket socket;
    private InetAddress group;
    private byte[] buf;
    protected int port = 6666;

    @Override
    public void run() {
        try {

            // socket = new DatagramSocket();
            socket = new MulticastSocket();
            group = InetAddress.getByName(MULTICAST_ADDRESS);
            socket.setReuseAddress(true);
            System.out.println("Sender initialized...");
            while (true) {
                sendMessages(Element.getMessagesLists());
                Thread.sleep(5000);
           }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void sendMessages(HashMap<Integer,String> messagesList) throws IOException {
        if (messagesList.isEmpty()) {
            System.out.println("No messages to send");
            return;
        }

        for(String message : messagesList.values()){
            buf = message.getBytes();

            DatagramPacket packet = new DatagramPacket(buf, buf.length, group, port);
            System.out.println(new String (packet.getData()) + '\n');
            socket.send(packet);
            System.out.println("Sent: " + message);
        }
        socket.close();
    }

}
*/