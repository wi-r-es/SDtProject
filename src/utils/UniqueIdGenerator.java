package utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class UniqueIdGenerator {
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
        public static String generateOperationId(String nodeId, String operationType) { //this one is more distributed friendly
            long timestamp = System.currentTimeMillis();
            return nodeId + "-" + operationType + "-" + timestamp;
        }
}
