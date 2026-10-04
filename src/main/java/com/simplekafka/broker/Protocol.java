package com.simplekafka.broker;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Protocol {
    // Client request types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    // Broker response types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;
    public static final byte ERROR_RESPONSE = 0x1F;

    // Internal broker communication
    public static final byte REPLICATE = 0x21;
    public static final byte REPLICATE_ACK = 0x22;
    public static final byte TOPIC_NOTIFICATION = 0x23;

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

    /**
     * Serializes a metadata request: "tell me about the cluster".
     * The broker answers with all brokers (id, host, port) and, for every topic,
     * each partition's leader and replicas. Clients use it to find which broker to talk to.
     * <p>
     * Wire layout: [type:1]. No fields, the request type alone is the whole question.
     * See notes/stage-2-protocol.md for an example.
     */
    public static ByteBuffer encodeMetadataRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    /**
     * Serializes a create-topic request: "create topic X with N partitions, each stored on R brokers".
     * <p>
     * Wire layout:
     * [type:1][topicLen:2][topic:N][numPartitions:4][replicationFactor:2]
     * <p>
     * Fixed part = 1 + 2 + 4 + 2 = 9 bytes.
     * See notes/stage-2-protocol.md for an example.
     */
    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(9 + topicBytes.length);
        buffer.put(CREATE_TOPIC);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);
        buffer.flip();
        return buffer;
    }

    /**
     * Serializes a replicate request: broker-to-broker, sent by a partition's leader to its followers
     * so they store a copy of the message at the same offset.
     * <p>
     * Wire layout:
     * [type:1][topicLen:2][topic:N][partition:4][offset:8][msgLen:4][message:M]
     * <p>
     * Fixed part = 1 + 2 + 4 + 8 + 4 = 19 bytes.
     * See notes/stage-2-protocol.md for an example.
     */
    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(19 + topicBytes.length + message.length);
        buffer.put(REPLICATE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }

    /**
     * Serializes a topic notification: broker-to-broker, sent by the controller after creating a topic,
     * telling other brokers "topic X now exists, load its details". Only the name is sent;
     * the partition assignments are read from the shared metadata store (ZooKeeper).
     * <p>
     * Wire layout: [type:1][topicLen:2][topic:N]
     * <p>
     * Fixed part = 1 + 2 = 3 bytes.
     * See notes/stage-2-protocol.md for an example.
     */
    public static ByteBuffer encodeTopicNotification(String topic) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(3 + topicBytes.length);
        buffer.put(TOPIC_NOTIFICATION);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.flip();
        return buffer;
    }

    /**
     * Turns the broker's answer to a produce request back into a {@link ProduceResult}.
     * <p>
     * Two possible layouts, chosen by the first byte:
     * [PRODUCE_RESPONSE:1][offset:8][status:1]          status 0 = ok
     * [ERROR_RESPONSE:1][errorLen:2][error:N]
     * <p>
     * See notes/stage-2-protocol.md for examples.
     */
    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != PRODUCE_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes, StandardCharsets.UTF_8);
                return new ProduceResult(-1, error);
            }
            return new ProduceResult(-1, "Invalid response type");
        }

        long offset = buffer.getLong();
        byte status = buffer.get();

        return new ProduceResult(offset, status == 0 ? null : "Produce failed");
    }

        /**
         * Result record for produce operations
         */
        public record ProduceResult(long offset, String error) {

        public boolean isSuccess() {
                return error == null;
            }
        }
}
