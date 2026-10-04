package com.simplekafka.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

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

    public static void sendErrorResponse(SocketChannel channel, String errorMessage) throws IOException {
        byte[] topicBytes = errorMessage.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(3 + topicBytes.length);
        buffer.put(ERROR_RESPONSE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.flip();
        channel.write(buffer);
    }

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
     * Turns the broker's answer to a fetch request into a {@link FetchResult}.
     * <p>
     * Two possible layouts, chosen by the first byte:
     * [FETCH_RESPONSE:1][count:4] then count x [offset:8][size:4][message:M]
     * [ERROR_RESPONSE:1][errorLen:2][error:N]
     * <p>
     * count = 0 is not an error: there are no new messages yet.
     * See notes/stage-2-protocol.md for examples.
     */
    public static FetchResult decodeFetchResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != FETCH_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes, StandardCharsets.UTF_8);
                return new FetchResult(new byte[0][], error);
            }
            return new FetchResult(new byte[0][], "Invalid response type");
        }

        int messageCount = buffer.getInt();
        byte[][] messages = new byte[messageCount][];

        for (int i = 0; i < messageCount; i++) {
            long offset = buffer.getLong(); // Skip offset
            int messageSize = buffer.getInt();
            messages[i] = new byte[messageSize];
            buffer.get(messages[i]);
        }

        return new FetchResult(messages, null);
    }

    /**
     * Turns the broker's answer to a metadata request into a {@link MetadataResult}.
     * <p>
     * Layout (each list starts with a count):
     * [METADATA_RESPONSE:1]
     * [brokerCount:4]  per broker:    [id:4][hostLen:2][host:N][port:4]
     * [topicCount:4]   per topic:     [topicLen:2][topic:N][partitionCount:4]
     * per partition: [partitionId:4][leaderId:4][followerCount:4][followerId:4]...
     * <p>
     * Or an error: [ERROR_RESPONSE:1][errorLen:2][error:N]
     * See notes/stage-2-protocol.md for an example.
     */
    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != METADATA_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes, StandardCharsets.UTF_8);
                return new MetadataResult(new ArrayList<>(), new ArrayList<>(), error);
            }
            return new MetadataResult(new ArrayList<>(), new ArrayList<>(), "Invalid response type");
        }

        // Parse broker info
        int brokerCount = buffer.getInt();
        List<BrokerInfo> brokers = new ArrayList<>();

        for (int i = 0; i < brokerCount; i++) {
            int brokerId = buffer.getInt();
            short hostLength = buffer.getShort();
            byte[] hostBytes = new byte[hostLength];
            buffer.get(hostBytes);
            String host = new String(hostBytes, StandardCharsets.UTF_8);
            int port = buffer.getInt();

            brokers.add(new BrokerInfo(brokerId, host, port));
        }

        // Parse topic metadata
        int topicCount = buffer.getInt();
        List<TopicMetadata> topics = new ArrayList<>();

        for (int i = 0; i < topicCount; i++) {
            short topicLength = buffer.getShort();
            byte[] topicBytes = new byte[topicLength];
            buffer.get(topicBytes);
            String topicName = new String(topicBytes, StandardCharsets.UTF_8);

            int partitionCount = buffer.getInt();
            List<PartitionMetadata> partitions = new ArrayList<>();

            for (int j = 0; j < partitionCount; j++) {
                int partitionId = buffer.getInt();
                int leaderId = buffer.getInt();

                int replicas = buffer.getInt();
                List<Integer> replicaIds = new ArrayList<>();

                for (int k = 0; k < replicas; k++) {
                    replicaIds.add(buffer.getInt());
                }

                partitions.add(new PartitionMetadata(partitionId, leaderId, replicaIds));
            }

            topics.add(new TopicMetadata(topicName, partitions));
        }

        return new MetadataResult(brokers, topics, null);
    }

    /**
     * Result record for produce operations
     */
    public record ProduceResult(long offset, String error) {

        public boolean isSuccess() {
            return error == null;
        }
    }

    /**
     * Result record for fetch operations
     */
    public record FetchResult(byte[][] messages, String error) {

        public int messageCount() {
            return messages.length;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    /**
     * Result record for metadata operations
     */
    public record MetadataResult(List<BrokerInfo> brokers, List<TopicMetadata> topics, String error) {

        public boolean isSuccess() {
            return error == null;
        }
    }

    /**
     * Topic metadata record
     */
    public record TopicMetadata(String name, List<PartitionMetadata> partitions) {
    }

    /**
     * Partition metadata record
     */
    public record PartitionMetadata(int id, int leader, List<Integer> replicas) {
    }
}
