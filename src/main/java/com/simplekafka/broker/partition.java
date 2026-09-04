package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

public class partition {
    private static final Logger LOGGER = Logger.getLogger(Partition.class.getName());
    private static final int DEFAULT_SEGMENT_SIZE = 1024 * 1024; // 1MB segment size
    private static final String LOG_SUFFIX = ".log";
    private static final String INDEX_SUFFIX = ".index";
    private final int id; // Unique partition identifier
    private int leader; // Leader broker ID
    private List<Integer> followers; // Follower broker IDs for replication
    private final String baseDir; // Directory for log storage
    private final AtomicLong nextOffset; // Next available message offset
    private final ReadWriteLock lock; // Concurrency control mechanism
    private RandomAccessFile activeLogFile; // Currently active log file
    private FileChannel activeLogChannel; // Channel for file operations
    private final List<SegmentInfo> segments; // List of segments in the partition

    public long append(byte[] message) {
        // Acquire write lock
        // Check if new segment needed
        // Write message size and data
        // Force write to disk
        // Update index
        // Increment offset counter
        // Return assigned offset
        lock.writeLock().lock();
        try {
            long currentOffset = nextOffset.get();
            if (activeLogFile.position() >= DEFAULT_SEGMENT_SIZE) {
                activeLogChannel.close();
                activeLogFile.close();
                createNewSegment(currentOffset);

            }
            // prepare the buffer for writing the message
            ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
            buffer.putInt(message.length);
            buffer.put(message);
            buffer.flip();

            long position = activeLogChannel.position();
            // write the message to the log file
            activeLogChannel.write(buffer);

            activeLogChannel.force(true); // Force write to disk
            // update the index with the current offset and position
            updateIndex(currentOffset, position);

            // Update offset
            nextOffset.incrementAndGet();

            return currentOffset;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void createNewSegment(long startOffset) {
        String baseName = String.format("%020d", baseOffset);
        String logPath = baseDir + File.separator + baseName + LOG_SUFFIX;
        String indexPath = baseDir + File.separator + baseName + INDEX_SUFFIX;

        File logFile = new File(logPath);
        logFile.createNewFile();

        File indexFile = new File(indexPath);
        indexFile.createNewFile();
        SegmentInfo segmentInfo = new SegmentInfo(baseOffset, logFile, indexFile);
        segments.add(segmentInfo);

        openSegmentForAppend(segment);

        LOGGER.info("Created new segment for partition " + id + ", base offset: " + baseOffset);

    }

    private void openSegmentForAppend(SegmentInfo segment) throws IOException {
        // Close the currently active segment if any
        if (activeLogChannel != null && activeLogChannel.isOpen()) {
            activeLogChannel.close();
        }

        if (activeLogFile != null) {
            activeLogFile.close();
        }

        // Open the segment
        activeLogFile = new RandomAccessFile(segment.getLogPath(), "rw");
        activeLogChannel = activeLogFile.getChannel();

        // Move to the end of the file for appending
        activeLogChannel.position(activeLogChannel.size());
    }

    public void updateIndex(long offset, long position) {
        // Update the index for the current segment
        try {
            if (segments.isEmpty())
                return;
            SegmentInfo currentSegment = segments.get(segments.size() - 1);

            try (RandomAccessFile indexFile = new RandomAccessFile(currentSegment.getIndexPath(), "rw")) {
                FileChannel indexChannel = indexFile.getChannel();
                indexChannel.position(indexChannel.size());
                ByteBuffer buffer = ByteBuffer.allocate(16);
                buffer.putLong(offset);
                buffer.putLong(position);
                buffer.flip();
                indexChannel.write(buffer);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public List<byte[]> readMessages(long offset, int maxBytes) {
        // Acquire read lock
        // Find segment containing offset
        // Find file position using index
        // Read messages until maxBytes limit
        // Handle segment boundaries
        // Return message list

        lock.readLock().lock();
        List<byte[]> messages = new ArrayList<>();
        int bytesRead = 0;
        try {
            SegmentInfo targetSegment = findSegmentForOffset(offset);
            if (targetSegment == null) {
                throw new RuntimeException("Segment not found for offset: " + offset);
            }
            long position = targetSegment.getPositionForOffset(offset);
            if (position < 0) {
                throw new RuntimeException("Invalid position for offset: " + offset);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            lock.readLock().unlock();
        }
        return messages;
    }

    private SegmentInfo findSegmentForOffset(long offset) {
        if (segments.isEmpty() || offset >= nextOffset.get()) {
            return null;
        }

        // Binary search to find the segment
        int low = 0;
        int high = segments.size() - 1;

        while (low <= high) {
            int mid = (low + high) / 2;
            SegmentInfo segment = segments.get(mid);

            if (mid < segments.size() - 1) {
                SegmentInfo nextSegment = segments.get(mid + 1);
                if (offset >= segment.getBaseOffset() && offset < nextSegment.getBaseOffset()) {
                    return segment;
                }
            } else {
                // Last segment
                if (offset >= segment.getBaseOffset()) {
                    return segment;
                }
            }

            if (offset < segment.getBaseOffset()) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        return null;
    }
}
