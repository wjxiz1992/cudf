package ai.rapids.cudf.serde.kudo;

import ai.rapids.cudf.*;
import ai.rapids.cudf.schema.SchemaVisitor;
import ai.rapids.cudf.utils.Arms;

import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ai.rapids.cudf.serde.kudo.KudoSerializer.padFor64byteAlignment;
import static ai.rapids.cudf.utils.PreConditions.ensure;
import static java.lang.Math.min;

public class MultiTableDeserializer implements SchemaVisitor<HostColumnVector, List<HostColumnVector>>, AutoCloseable {
    // Number of 1s in a byte
    private static final int[] NUMBER_OF_ONES = new int[256];

    static {
        for (int i = 0; i < NUMBER_OF_ONES.length; i += 1) {
            int count = 0;
            for (int j = 0; j < 8; j += 1) {
                if ((i & (1 << j)) != 0) {
                    count += 1;
                }
            }
            NUMBER_OF_ONES[i] = count;
        }
    }

    private final List<SerializedTable> tables;
    private final long[] currentValidityOffsets;
    private final long[] currentOffsetOffsets;
    private final long[] currentDataOffset;
    private final Deque<SliceInfo>[] sliceInfoStack;
    private final Deque<Long> totalRowCountStack;
    private int currentIdx;
    // Temporary buffer to store the slice info for each table to avoid repeated allocation
    private final SliceInfo[] outputSliceInfo;

    public MultiTableDeserializer(List<SerializedTable> tables) {
        Objects.requireNonNull(tables, "tables cannot be null");
        ensure(!tables.isEmpty(), "tables cannot be empty");
        this.tables = tables;
        this.currentValidityOffsets = new long[tables.size()];
        this.currentOffsetOffsets = new long[tables.size()];
        this.currentDataOffset = new long[tables.size()];
        this.sliceInfoStack = new Deque[tables.size()];
        for (int i = 0; i < tables.size(); i++) {
            this.currentValidityOffsets[i] = 0;
            SerializedTableHeader header = tables.get(i).getHeader();
            this.currentOffsetOffsets[i] = header.getValidityBufferLen();
            this.currentDataOffset[i] = header.getValidityBufferLen() + header.getOffsetBufferLen();
            this.sliceInfoStack[i] = new ArrayDeque<>(16);
            this.sliceInfoStack[i].add(new SliceInfo(header.getOffset(), header.getNumRows()));
        }
        long totalRowCount = tables.stream().mapToLong(t -> t.getHeader().getNumRows()).sum();
        this.totalRowCountStack = new ArrayDeque<>(16);
        totalRowCountStack.addLast(totalRowCount);
        this.currentIdx = 0;
        this.outputSliceInfo = new SliceInfo[tables.size()];
    }

    private long getCurrentTotalRowCount() {
        return totalRowCountStack.getLast();
    }

    @Override
    public List<HostColumnVector> visitTopSchema(Schema schema, List<HostColumnVector> children) {
        return children;
    }

    @Override
    public HostColumnVector visitStruct(Schema structType, List<HostColumnVector> children) {
        AtomicLong nullCount = new AtomicLong(0);
        HostMemoryBuffer validityBuffer = deserializeValidityBuffer(nullCount);

        currentIdx += 1;

        return new HostColumnVector(structType.getType(),
                getCurrentTotalRowCount(),
                Optional.of(nullCount.get()),
                null,
                validityBuffer,
                null,
                children.stream().map(h -> (HostColumnVectorCore) h).collect(Collectors.toList()));
    }

    @Override
    public HostColumnVector preVisitList(Schema listType) {
        AtomicLong nullCount = new AtomicLong(0);
        HostMemoryBuffer validityBuffer = deserializeValidityBuffer(nullCount);
        long listRowCount = getCurrentTotalRowCount();
        AtomicLong outputTotalRowCount = new AtomicLong(0);
        HostMemoryBuffer offsetBuffer = deserializeOffsetBuffer(outputTotalRowCount);


        for (int tableIdx = 0; tableIdx < tables.size(); tableIdx += 1) {
            sliceInfoStack[tableIdx].addLast(outputSliceInfo[tableIdx]);
        }

        this.totalRowCountStack.addLast(outputTotalRowCount.get());
        currentIdx += 1;

        return new HostColumnVector(listType.getType(),
                listRowCount,
                Optional.of(nullCount.get()),
                null,
                validityBuffer,
                offsetBuffer,
                Collections.emptyList());
    }

    @Override
    public HostColumnVector visitList(Schema listType, HostColumnVector preVisitResult, HostColumnVector childResult) {
        try (HostColumnVector prevList = preVisitResult) {
            for (int tableIdx = 0; tableIdx < tables.size(); tableIdx += 1) {
                sliceInfoStack[tableIdx].removeLast();
            }
            totalRowCountStack.removeLast();

            HostMemoryBuffer[] tmpBuffer = new HostMemoryBuffer[2];
            return Arms.closeIfException(CloseableArray.wrap(tmpBuffer), buffers -> {
                HostMemoryBuffer validityBuffer = prevList.getValidity();
                validityBuffer.incRefCount();
                tmpBuffer[0] = validityBuffer;

                HostMemoryBuffer offsetBuffer = prevList.getOffsets();
                offsetBuffer.incRefCount();
                tmpBuffer[1] = offsetBuffer;

                return new HostColumnVector(listType.getType(),
                        preVisitResult.getRowCount(),
                        Optional.of(preVisitResult.getNullCount()),
                        null,
                        preVisitResult.getValidity(),
                        preVisitResult.getOffsets(),
                        Collections.singletonList(childResult));
            });
        }
    }

    @Override
    public HostColumnVector visit(Schema primitiveType) {
        AtomicLong nullCount = new AtomicLong(0);
        int[] dataLen = new int[tables.size()];
        long totalDataLen = 0;
        HostMemoryBuffer validityBuffer = deserializeValidityBuffer(nullCount);
        HostMemoryBuffer offsetBuffer = null;
        if (primitiveType.getType().hasOffsets()) {
            AtomicLong totalDataLenHolder = new AtomicLong(0);
            offsetBuffer = deserializeOffsetBuffer(totalDataLenHolder);
            for (int i = 0; i < tables.size(); i += 1) {
                dataLen[i] = (int) (outputSliceInfo[i].getRowCount());
            }
            totalDataLen = totalDataLenHolder.get();
        } else {
            for (int i = 0; i < tables.size(); i += 1) {
                dataLen[i] = (int) (sliceInfoStack[i].getLast().getRowCount() *
                        primitiveType.getType().getSizeInBytes());
                totalDataLen += dataLen[i];
            }
        }


        HostMemoryBuffer dataBuffer = deserializeDataBuffer(dataLen, totalDataLen);

        this.currentIdx += 1;
        return Arms.closeIfException(new HostColumnVector(primitiveType.getType(),
                getCurrentTotalRowCount(),
                Optional.of(nullCount.get()),
                dataBuffer,
                validityBuffer,
                offsetBuffer,
                Collections.emptyList()), Function.identity());
    }

    private HostMemoryBuffer deserializeValidityBuffer(AtomicLong nullCount) {
        long validityBufferSize = padFor64byteAlignment(BitVectorHelper
                .getValidityLengthInBytes(getCurrentTotalRowCount()));
        return Arms.closeIfException(HostMemoryBuffer.allocate(validityBufferSize), buffer -> {
            int nullCountTotal = 0;
            int startRow = 0;
            for (int tableIdx = 0; tableIdx < tables.size(); tableIdx += 1) {
                SerializedTable table = tables.get(tableIdx);
                SliceInfo sliceInfo = sliceInfoStack[tableIdx].getLast();

                long thisBufferLen = 0;

                if (table.getHeader().hasValidityBuffer(currentIdx)) {
                    thisBufferLen = sliceInfo.getValidityBufferInfo().getBufferLength();
                    try (HostMemoryBuffer thisValidityBuffer = table.getBuffer().slice(currentValidityOffsets[tableIdx], thisBufferLen)) {
                        nullCountTotal += copyValidityBuffer(buffer, startRow, thisValidityBuffer, sliceInfo);
                    }
                } else {
                    appendAllValid(buffer, startRow, sliceInfo.getRowCount());
                    nullCountTotal += 0;
                }

                currentValidityOffsets[tableIdx] += thisBufferLen;
                startRow += (int) sliceInfo.getRowCount();
            }

            nullCount.set(nullCountTotal);

            return buffer;
        });
    }

    /**
     * Copy a sliced validity buffer to the destination buffer, starting at the given bit offset.
     *
     * @return Number of nulls in the validity buffer.
     */
    private static int copyValidityBuffer(HostMemoryBuffer dest, int startBit, HostMemoryBuffer thisBuffer, SliceInfo sliceInfo) {
        int nullCount = 0;
        int totalRowCount = (int) sliceInfo.getRowCount();
        int curIdx = 0;
        int curSrcByteIdx = 0;
        int curSrcBitIdx = (int) sliceInfo.getValidityBufferInfo().getBeginBit();
        int curDestByteIdx = startBit / 8;
        int curDestBitIdx = startBit % 8;

        while (curIdx < totalRowCount) {
            int leftRowCount = totalRowCount - curIdx;
            int appendCount;
            if (curDestBitIdx == 0) {
                appendCount = min(8, leftRowCount);
            } else {
                appendCount = min(8 - curDestBitIdx, leftRowCount);
            }

            int leftBitsInCurSrcByte = 8 - curSrcBitIdx;
            byte srcByte = thisBuffer.getByte(curSrcByteIdx);
            if (leftBitsInCurSrcByte >= appendCount) {
                // Extract appendCount bits from srcByte, starting from curSrcBitIdx
                byte mask = (byte) ((1 << appendCount) - 1);
                srcByte = (byte) ((srcByte >> curSrcBitIdx) & mask);

                nullCount += (appendCount - NUMBER_OF_ONES[srcByte & 0xFF]);

                // Sets the bits in destination buffer starting from curDestBitIdx to 0
                byte destByte = dest.getByte(curDestByteIdx);
                destByte = (byte) (destByte & ((1 << curDestBitIdx) - 1));

                // Update destination byte with the bits from source byte
                destByte = (byte) (destByte | (srcByte << curDestBitIdx));
                dest.setByte(curDestByteIdx, destByte);

                curSrcBitIdx += appendCount;
                if (curSrcBitIdx == 8) {
                    curSrcBitIdx = 0;
                    curSrcByteIdx += 1;
                }
            } else {
                srcByte = (byte) (srcByte >> curSrcBitIdx);

                byte nextSrcByte = thisBuffer.getByte(curSrcByteIdx + 1);
                byte nextSrcByteMask = (byte) ((1 << (appendCount - leftBitsInCurSrcByte)) - 1);
                nextSrcByte = (byte) (nextSrcByte & nextSrcByteMask);
                nextSrcByte = (byte) (nextSrcByte << leftBitsInCurSrcByte);
                srcByte = (byte) (srcByte | nextSrcByte);

                nullCount += (appendCount - NUMBER_OF_ONES[srcByte & 0xFF]);

                // Sets the bits in destination buffer starting from curDestBitIdx to 0
                byte destByte = dest.getByte(curDestByteIdx);
                destByte = (byte) (destByte & ((1 << curDestBitIdx) - 1));

                // Update destination byte with the bits from source byte
                destByte = (byte) (destByte | (srcByte << curDestBitIdx));
                dest.setByte(curDestByteIdx, destByte);

                // Update the source byte index and bit index
                curSrcByteIdx += 1;
                curSrcBitIdx = appendCount - leftBitsInCurSrcByte;
            }

            curIdx += appendCount;

            // Update the destination byte index and bit index
            curDestBitIdx += appendCount;
            if (curDestBitIdx == 8) {
                curDestBitIdx = 0;
                curDestByteIdx += 1;
            }
        }

        return nullCount;
    }

    private static void appendAllValid(HostMemoryBuffer dest, int startBit, long numRows) {
        int curDestByteIdx = startBit / 8;
        int curDestBitIdx = startBit % 8;
        int curIdx = 0;
        while (curIdx < numRows) {
            int leftRowCount = (int) numRows - curIdx;
            int appendCount;
            if (curDestBitIdx == 0) {
                dest.setByte(curDestByteIdx, (byte) 0xFF);
                appendCount = min(8, leftRowCount);
            } else {
                appendCount = min(8 - curDestBitIdx, leftRowCount);
                byte mask = (byte) (((1 << appendCount) - 1) << curDestBitIdx);
                byte destByte = dest.getByte(curDestByteIdx);
                dest.setByte(curDestByteIdx, (byte) (destByte | mask));
            }

            curDestBitIdx += appendCount;
            if (curDestBitIdx == 8) {
                curDestBitIdx = 0;
                curDestByteIdx += 1;
            }

            curIdx += appendCount;
        }
    }

    private HostMemoryBuffer deserializeOffsetBuffer(AtomicLong outputTotalRowCount) {
        long bufferSize = Integer.BYTES * (getCurrentTotalRowCount() + 1);
        return Arms.closeIfException(HostMemoryBuffer.allocate(bufferSize), buffer -> {
            IntBuffer buf = buffer
                    .asByteBuffer(0L, (int) bufferSize)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .asIntBuffer();
            int idx = 0;
            int accumulatedDataLen = 0;
            for (int tableIdx = 0; tableIdx < tables.size(); tableIdx += 1) {
                SerializedTable table = tables.get(tableIdx);
                SliceInfo sliceInfo = sliceInfoStack[tableIdx].getLast();

                if (sliceInfo.getRowCount() > 0) {
                    long thisBufferLen = Integer.BYTES * (sliceInfo.getRowCount() + 1);

                    try (HostMemoryBuffer thisOffsetBuffer = table.getBuffer().slice(currentOffsetOffsets[tableIdx], thisBufferLen)) {
                        IntBuffer thisBuf = thisOffsetBuffer
                                .asByteBuffer()
                                .order(ByteOrder.LITTLE_ENDIAN)
                                .asIntBuffer();
                        int firstOffset = thisBuf.get(0);
                        int lastOffset = thisBuf.get((int) sliceInfo.getRowCount());

                        for (int i = 0; i < sliceInfo.getRowCount(); i += 1) {
                            buf.put(idx, thisBuf.get() - firstOffset + accumulatedDataLen);
                            idx += 1;
                        }
                        outputSliceInfo[tableIdx] = new SliceInfo(firstOffset, lastOffset - firstOffset);
                        accumulatedDataLen += (int) outputSliceInfo[tableIdx].getRowCount();
                    }

                    currentOffsetOffsets[tableIdx] += thisBufferLen;
                } else {
                    outputSliceInfo[tableIdx] = new SliceInfo(0, 0);
                }
            }

            buf.put(idx, accumulatedDataLen);
            outputTotalRowCount.set(accumulatedDataLen);
            return buffer;
        });
    }

    private HostMemoryBuffer deserializeDataBuffer(int[] dataLen, long totalDataLen) {
        if (totalDataLen == 0) {
            // See https://github.com/rapidsai/cudf/blob/f049d6c6a90dce032a1cb4d2a8c36f280dffc43f/java/src/main/java/ai/rapids/cudf/HostColumnVector.java#L239
            return HostMemoryBuffer.allocate(1);
        }
        return Arms.closeIfException(HostMemoryBuffer.allocate(totalDataLen), buffer -> {
            long start = 0;
            for (int tableIdx = 0; tableIdx < tables.size(); tableIdx += 1) {
                SerializedTable table = tables.get(tableIdx);

                try (HostMemoryBuffer thisDataBuffer = table.getBuffer().slice(currentDataOffset[tableIdx], dataLen[tableIdx])) {
                    buffer.copyFromHostBuffer(start, thisDataBuffer, 0, dataLen[tableIdx]);
                    start += dataLen[tableIdx];
                }
                currentDataOffset[tableIdx] += dataLen[tableIdx];
            }

            return buffer;
        });
    }

    @Override
    public void close() throws Exception {
        if (tables != null) {
            Arms.closeQuietly(tables);
        }
    }

    @Override
    public String toString() {
        return "MultiTableDeserializer{" +
                "tables=" + tables +
                ", currentValidityOffsets=" + Arrays.toString(currentValidityOffsets) +
                ", currentOffsetOffsets=" + Arrays.toString(currentOffsetOffsets) +
                ", currentDataOffset=" + Arrays.toString(currentDataOffset) +
                ", sliceInfoStack=" + Arrays.toString(sliceInfoStack) +
                ", totalRowCountStack=" + totalRowCountStack +
                ", currentIdx=" + currentIdx +
                ", outputSliceInfo=" + Arrays.toString(outputSliceInfo) +
                '}';
    }
}
