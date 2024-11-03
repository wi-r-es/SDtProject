import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class MulticastTest {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: java MulticastTest [sender|receiver]");
            return;
        }

        if (args[0].equals("sender")) {
            DatagramSocket socket = new DatagramSocket();
            InetAddress group = InetAddress.getByName("230.23.23.23");
            String msg = "Test message";

            while (true) {
                DatagramPacket packet = new DatagramPacket(
                        msg.getBytes(), msg.length(), group, 2323);
                socket.send(packet);
                System.out.println("Sent: " + msg);
                Thread.sleep(1000);
            }
        } else {
            MulticastSocket socket = new MulticastSocket(2323);
            InetAddress group = InetAddress.getByName("230.23.23.23");
            socket.joinGroup(group);

            while (true) {
                byte[] buf = new byte[1024];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                System.out.println("Received: " +
                        new String(packet.getData(), 0, packet.getLength()));
            }
        }
    }
}