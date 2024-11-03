import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Date;

public class ReceiveListener extends Thread {
    private static final String MULTICAST_ADDRESS = "230.23.23.23";
    private static final int PORT = 2323;
    private MulticastSocket socket;
    private InetAddress group;

    @Override
    public void run() {
        try {
            socket = new MulticastSocket(PORT);
            group = InetAddress.getByName(MULTICAST_ADDRESS);
            socket.joinGroup(group);

            while (true) {
                receiveMessage();
            }
            socket.leaveGroup(group);
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void receiveMessage() throws IOException, IOException {
        System.out.println("im here recieve message");
        byte[] buf = new byte[1024];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        socket.receive(packet);

        String received = new String(packet.getData(), 0, packet.getLength());
        processMessage(received);
        if ("end".equals(received)) {
            break;
        }
    }
    /** Process received message **/
    private void processMessage(String message) {
        System.out.println("[Node " + Thread.currentThread().getName() + "] Received: " + message);
        System.out.println("Received at: " + new Date());
        System.out.println("Received message: " + message);

    }
}