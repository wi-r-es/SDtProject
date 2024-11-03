public class Main {
    public static void main(String[] args) {
        try {
            // Create test scenario based on command line argument
            if (args.length > 0) {
                int nodeType = Integer.parseInt(args[0]);
                runNode(nodeType);
            } else {
                System.out.println("Please specify node type: 1 for leader, 2 for regular element");
            }
        } catch (NumberFormatException e) {
            System.out.println("Invalid argument. Please use: 1 for leader, 2 for regular element");
        }
    }

    private static void runNode(int nodeType) {
        if (nodeType == 1) {
            // Leader node
            Element leader = new Element(1);
            leader.start();

            // Simulate adding messages periodically
            new Thread(() -> {
                int messageCounter = 1;
                while (true) {
                    try {
                        String message = "Message " + messageCounter + " from leader at " + System.currentTimeMillis();
                        leader.addMessage(messageCounter, message);
                        System.out.println("Leader sent: " + message);
                        messageCounter++;
                        Thread.sleep(7000); // Add new message every 7 seconds
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();

        } else {
            // Regular element
            Element element = new Element(0);
            element.start();
            System.out.println("Started regular element - listening for messages...");
        }
    }
}