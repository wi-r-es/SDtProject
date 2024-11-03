import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Date;

public class ReceiveListener extends Thread {
    private static final String MULTICAST_ADDRESS = "230.23.23.23";
    protected MulticastSocket socket = null;
    byte[] buf = new byte[1024];

    @Override
    public void run() {
        try {
            final int PORT = 2323;
            socket = new MulticastSocket(PORT);
            InetAddress group = InetAddress.getByName(MULTICAST_ADDRESS);
            socket.joinGroup(group);

            while (true) {
                System.out.println("im here receive message");
                //receiveMessage();
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                System.out.println("im here now");
                String received = new String(packet.getData(), 0, packet.getLength());
                processMessage(received);
                if ("end".equals(received)) {
                    break;
                }
            }
            System.out.println("going to leave" );
            socket.leaveGroup(group);
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
/*
    private void receiveMessage() throws IOException, IOException {
        System.out.println("im here receive message");
        byte[] buf = new byte[1024];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        socket.receive(packet);

        String received = new String(packet.getData(), 0, packet.getLength());
        processMessage(received);
        if ("end".equals(received)) {
            break;
        }
    }
    */

    /** Process received message **/
    private void processMessage(String message) {
        System.out.println("[Node " + Thread.currentThread().getName() + "] Received: " + message);
        System.out.println("Received at: " + new Date());
        System.out.println("Received message: " + message);

    }
}