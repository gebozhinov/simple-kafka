package com.simplekafka.broker;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Protocol {
    // Client request types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;

    /**
     * Serializes a produce request into bytes ready to be written to a socket.
     * <p>
     * Wire layout:
     * [type:1][topicLen:2][topic:N][partition:4][msgLen:4][message:M]
     * <p>
     * Fixed part = 1 + 2 + 4 + 4 = 11 bytes.
     * See notes/stage-2-protocol.md for a byte-by-byte example.
     */
    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(11 + topicBytes.length + message.length);
        buffer.put(PRODUCE);                         // 1 byte: request type
        buffer.putShort((short) topicBytes.length);  // 2 bytes: tells the reader how many topic bytes follow
        buffer.put(topicBytes);                      // N bytes: topic name
        buffer.putInt(partition);                    // 4 bytes
        buffer.putInt(message.length);               // 4 bytes: length prefix for the message
        buffer.put(message);                         // M bytes: payload
        // Switch from writing to reading: limit = position, position = 0.
        // Without it, channel.write(buffer) sees 0 bytes remaining and silently sends nothing.
        buffer.flip();
        return buffer;
    }

    /**
     * Serializes a fetch request: a consumer asking for messages from topic/partition,
     * starting at {@code offset}, returning at most {@code maxBytes}.
     * <p>
     * Wire layout:
     * [type:1][topicLen:2][topic:N][partition:4][offset:8][maxBytes:4]
     * <p>
     * Fixed part = 1 + 2 + 4 + 8 + 4 = 19 bytes.
     * See notes/stage-2-protocol.md for an example.
     */
    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(19 + topicBytes.length);
        buffer.put(FETCH);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);
        buffer.flip();
        return buffer;
    }

}
