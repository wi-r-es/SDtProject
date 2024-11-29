package utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class network {
    /// FOR IMPROVE READABILITY IN THE BROADCAST AND UNICAST ABOVE 
    /**
     * Serializes an object into a byte array.
     *
     * @param object The object to serialize.
     * @return The serialized byte array.
     * @throws IOException If an I/O error occurs during serialization.
     */
    public static byte[] serialize(Object object) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (ObjectOutputStream objectStream = new ObjectOutputStream(byteStream)) {
            objectStream.writeObject(object);
            objectStream.flush();
        }
        return byteStream.toByteArray();
    }
    /**
     * Deserializes a byte array into an object.
     *
     * @param data The byte array to deserialize.
     * @return The deserialized object.
     * @throws IOException If an I/O error occurs during deserialization.
     * @throws ClassNotFoundException If the class of the deserialized object is not found.
     */
    public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        try (ObjectInputStream objectStream = new ObjectInputStream(new ByteArrayInputStream(data))) {
            return objectStream.readObject();
        }
    }
    
    /**
     * Adds a header to the payload data.
     *
     * @param header The header to add.
     * @param payload The payload data.
     * @return The byte array with the added header.
     * @throws IOException If an I/O error occurs.
     */
    public static byte[] addHeader(String header, byte[] payload) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        byteStream.write(header.getBytes()); 
        byteStream.write(payload);           
        return byteStream.toByteArray();
    }
}
