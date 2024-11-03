// TestCases.java
public class TestCases {
    public static void main(String[] args) {
        try {
            // Test Case 1: Single Element Receiving
            System.out.println("Test Case 1: Single Element");
            Element element = new Element(0);
            element.start();
            Thread.sleep(10000);  // Wait 10 seconds

            // Test Case 2: Non-leader trying to send
            System.out.println("\nTest Case 2: Non-leader send attempt");
            element.addMessage(1, "This shouldn't be sent");
            Thread.sleep(5000);

            // Test Case 3: Multiple messages from leader
            System.out.println("\nTest Case 3: Multiple messages");
            Element leader = new Element(1);
            leader.start();
            for(int i = 1; i <= 5; i++) {
                leader.addMessage(i, "Message " + i);
                Thread.sleep(1000);
            }

            System.out.println("\nTests completed. Please stop the run.");
            while(true) {
                Thread.sleep(1000);
            }

        } catch (InterruptedException e) {
            System.out.println("Tests stopped.");
        }
    }
}