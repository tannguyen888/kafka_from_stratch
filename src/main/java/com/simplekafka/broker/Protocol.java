package com.simplekafka.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

public class Protocol {
    // stage 1 final variales for request and response types
    // Broker request types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    // Broker response types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte ERROR_RESPONSE = 0x13;

    // stage 2

    // buffer.get() to read the first byte of the buffer, which represents the
    // request type.
    // Then, based on the request type, it reads the corresponding fields from the
    // buffer and constructs a Request object.

    // buffer.put() to write the request type and the corresponding fields into a
    // ByteBuffer.
    // It then flips the buffer to prepare it for reading and returns it.

    // buffer.getInt() to read an integer value from the buffer, which represents
    // the offset of the produced message.

    // buffer.getShort() to read a short value from the buffer,
    // which represents the length of the topic name or the number of messages in a
    // fetch response.

    // In Byptes -> topic =N; topic.length = 2; regular put = 1 ; partition = 4 ;
    // offset = 8 ; maxBytes = 4 ; messageLength = 4 ; message = N

    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        ByteBuffer buffer = ByteBuffer.allocate(11 + topic.length() + message.length);
        buffer.put(PRODUCE);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(partition);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;

    }

    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        ByteBuffer buffer = ByteBuffer.allocate(19 + topic.length());
        buffer.put(FETCH);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);
        buffer.flip();
        return buffer;

    }

    public static ByteBuffer encodeMetadataRequest(List<String> topics) {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + topic.length() + 4 + 2);
        buffer.put(CREATE_TOPIC);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);
        buffer.flip();
        return buffer;
    }

    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != PRODUCE_RESPONSE) {

            throw new IllegalArgumentException("Invalid response type: " + responseType);
        }
        int offset = buffer.getInt();
        String status = buffer.get() == 0 ? "SUCCESS" : "FAILURE";
        return new ProduceResult(offset, status);

    }

public static FetchResult decodeFetchResponse(ByteBuffer buffer)
{
    byte responseType = buffer.get();
    if(responseType != FETCH_RESPONSE){
        throw new IllegalArgumentException("Invalid response type: " + responseType);
        if(reponseType == ERROR_RESPONSE){
            short errorLength = buffer.getShort();
            byte[] errorMessage = new byte[errorLength];
            buffer.get(errorMessage);
            String error = new String (errorMessage);
            return new FetchResult(byte[0], error);
        }
        return new FetchResult(byte[0]  , "Invalid response type: " + responseType);
    }
short messageCount = buffer.getShort();
        byte[][] message = new byte[messageCount][];
    for(int i =0 ; i < messageCount ; i++){
        long offset = buffer.getLong();
         message[i] = new byte[buffer.getInt()];
        buffer.get(message[i]);

    }
    return new FetchResult(message, "SUCCESS");
}
}