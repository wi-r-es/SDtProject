package utils;

import java.io.*;
import java.util.zip.*;
/**
 * Utility class for compression and decompression using GZIP.
 * 
 * @see https://medium.com/javarevisited/efficient-handling-and-processing-of-compressed-files-in-java-7d023551168c
 * @see https://blogs.oracle.com/javamagazine/post/curly-braces-java-network-transmission-compression
 * @see https://snowcloudbyte.medium.com/compression-and-decompression-data-with-java-3185f831b8b8
 * 
 */
public class CompressionUtils {

    /**
     * Compresses a byte array using GZIP.
     *
     * @param data The byte array to compress.
     * @return The compressed byte array.
     * @throws IOException If an I/O error occurs during compression.
     * @see ByteArrayOutputStream
     * @see GZIPOutputStream
     */
    public static byte[] compress(byte[] data) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
            gzipOutputStream.write(data);
        }
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * Decompresses a byte array using GZIP.
     *
     * @param compressedData The compressed byte array to decompress.
     * @return The decompressed byte array.
     * @throws IOException If an I/O error occurs during decompression.
     * @see ByteArrayInputStream
     * @see ByteArrayOutputStream
     * @see GZIPInputStream
     */
    public static byte[] decompress(byte[] compressedData) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(compressedData);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = gzipInputStream.read(buffer)) != -1) {
                byteArrayOutputStream.write(buffer, 0, bytesRead);
            }
        }
        return byteArrayOutputStream.toByteArray();
    }
}

