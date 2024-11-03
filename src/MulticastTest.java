import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class MulticastTest {
    public static void main(String[] args) {
        try {
            // Start leader
            Element leader = new Element(1);
            leader.start();
            System.out.println("Leader started");

            // Start regular node
            Element regular = new Element(0);
            regular.start();
            System.out.println("Regular node started");

            // Wait a bit
            Thread.sleep(2000);

            // Send some test messages
            for (int i = 1; i <= 3; i++) {
                leader.addMessage(i, "Test message " + i);
                Thread.sleep(1000);
            }

            // Keep running for a while
            Thread.sleep(20000);

            // Clean shutdown
            leader.stop();
            regular.stop();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}