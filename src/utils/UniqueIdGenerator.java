package utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class for generating unique operation IDs.
 */
public class UniqueIdGenerator {
    /**
     * Generates a unique operation ID based on the operation data.
     * Uses SHA-256 hashing to generate a unique identifier.
     *
     * @param operationData The data associated with the operation.
     * @return The unique operation ID.
     */
    public static String generateOperationId(String operationData) { //this one can be slow
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(operationData.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                hexString.append(String.format("%02x", b));
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error generating hash", e);
        }
    }
    /**
     * Generates a unique operation ID based on the node ID and operation type.
     * Uses a combination of node ID, operation type, and timestamp to generate a unique identifier.
     *
     * @param nodeId       The ID of the node.
     * @param operationType The type of the operation.
     * @return The unique operation ID.
     */
    public static String generateOperationId(String nodeId, String operationType) { //this one is more distributed friendly
            long timestamp = System.currentTimeMillis();
            return nodeId + "-" + operationType + "-" + timestamp;
    }
}
