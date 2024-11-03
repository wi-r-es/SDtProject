import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class ReceiveListener extends Thread {
    private static final String MULTICAST_ADDRESS = "230.0.0.1";
    private static final int PORT = 4446;
    private Element element;

    public ReceiveListener(Element element) {
        this.element = element;
    }

    private void replyToHeartbeat() {
        try (MulticastSocket socket = new MulticastSocket()) {
            String ackMessage = "I’m Alive " + element.getId();
            byte[] buffer = ackMessage.getBytes();
            InetAddress group = InetAddress.getByName(MULTICAST_ADDRESS);
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, PORT);
            socket.send(packet);
            System.out.println("Node " + element.getId() + " sent heartbeat acknowledgment.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try (MulticastSocket socket = new MulticastSocket(PORT)) {
            InetAddress group = InetAddress.getByName(MULTICAST_ADDRESS);
            socket.joinGroup(group);
            System.out.println("Receiver listening on multicast address " + MULTICAST_ADDRESS + " and port " + PORT);

            while (true) {
                byte[] buffer = new byte[256];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                String message = new String(packet.getData(), 0, packet.getLength());
                System.out.println("Node " + element.getId() + " received message: " + message);
                // Process heartbeat messages and send acknowledgment
                if (message.equals("heartbeat")) {
                    replyToHeartbeat();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}




/*import java.io.IOException;
import java.net.*;
import java.util.Date;

public class ReceiveListener extends Thread {
    private static final String MULTICAST_ADDRESS = "230.23.23.24";
    final int PORT = 6666;
    protected MulticastSocket socket = null;
    private InetAddress group;

    private boolean running = true;
    @Override
    public void run() {
        try {

            socket = new MulticastSocket(PORT);
            group = InetAddress.getByName(MULTICAST_ADDRESS);


            socket.setReuseAddress(true);
            socket.setBroadcast(true);
            NetworkInterface networkInterface = NetworkInterface.getByName("ens33");
            socket.joinGroup(group);

            System.out.println("Receiver started on port " + PORT);

            socket.joinGroup(new InetSocketAddress(group, PORT), networkInterface);

            // Set Time-To-Live and disable loopback mode
            socket.setTimeToLive(4);
            socket.setLoopbackMode(false);
            System.out.println("Joined multicast group, waiting for messages...");


            while (running) {

                System.out.println("Waiting for messages...");
                receiveMessage();

            }
            System.out.println("going to leave" );
            socket.leaveGroup(group);
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (socket != null) {
                try {
                    socket.leaveGroup(group);
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void receiveMessage() throws IOException, IOException {
        System.out.println("im here receive message");
        byte[] buf = new byte[1024];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        socket.receive(packet);
        System.out.println("Message receiveed");

        String received = new String(packet.getData(), 0, packet.getLength());
        processMessage(received);

    }


    /** Process received message
    private void processMessage(String message) {
        System.out.println("[Node " + Thread.currentThread().getName() + "] Received: " + message);
        System.out.println("Received at: " + new Date());
        System.out.println("Received message: " + message);

    }

    public void stopListener() {
        running = false;
    }
}

*/