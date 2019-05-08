package ru.mail.polis.brainlux;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class SSTable implements Table {
    private final int rows;
    private final IntBuffer offsets;
    private final ByteBuffer cells;
    private final long sizeInBytes;

    SSTable(final File file) throws IOException {
        this.sizeInBytes = file.length();
        assert sizeInBytes != 0 && sizeInBytes <= Integer.MAX_VALUE;
        final ByteBuffer mapped;
        try (FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0L, fc.size()).order(ByteOrder.BIG_ENDIAN);
        }

        //Rows
        rows = mapped.getInt((int) (sizeInBytes - Integer.BYTES));

        // Offset
        final ByteBuffer offsetBuffer = mapped.duplicate();
        offsetBuffer.position(mapped.limit() - Integer.BYTES * rows - Integer.BYTES);
        offsetBuffer.limit(mapped.limit() - Integer.BYTES);
        this.offsets = offsetBuffer.slice().asIntBuffer();

        // Cells
        final ByteBuffer cellBuffer = mapped.duplicate();
        cellBuffer.limit(offsetBuffer.position());
        this.cells = cellBuffer.slice();
    }

    @Override
    public long sizeInBytes() {
        return sizeInBytes;
    }

    /**
     * Writes MemTable data to disk.
     *
     * @param cells iterator of MemTable
     * @param to    path of the file where data needs to be written
     * @throws IOException if an I/O error occurred
     */

    static void write(final Iterator<Cell> cells, final File to) throws IOException {
        try (FileChannel fc = FileChannel.open(to.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            final List<Integer> offsets = new ArrayList<>();
            int offset = 0;
            while (cells.hasNext()) {
                offsets.add(offset);

                final Cell cell = cells.next();

                //Key
                final ByteBuffer key = cell.getKey();
                final int keySize = cell.getKey().remaining();
                fc.write(Bytes.fromInt(keySize));
                offset += Integer.BYTES;
                fc.write(key);
                offset += keySize;

                //Value
                final Value value = cell.getValue();

                //Timestamp
                if (value.isRemoved()) {
                    fc.write(Bytes.fromLong(-cell.getValue().getTimeStamp()));
                } else {
                    fc.write(Bytes.fromLong(cell.getValue().getTimeStamp()));
                }
                offset += Long.BYTES;

                //Value
                if (!value.isRemoved()) {
                    final ByteBuffer valueData = value.getData();
                    final int valueSize = value.getData().remaining();
                    fc.write(Bytes.fromInt(valueSize));
                    offset += Integer.BYTES;
                    fc.write(valueData);
                    offset += valueSize;
                }
            }

            // Offsets
            for (final Integer anOffset : offsets) {
                fc.write(Bytes.fromInt(anOffset));
            }

            //Cells
            fc.write(Bytes.fromInt(offsets.size()));
        }
    }

    private ByteBuffer keyAt(final int i) {
        assert 0 <= i && i < rows;
        final int offset = offsets.get(i);
        final int keySize = cells.getInt(offset);
        final ByteBuffer key = cells.duplicate();
        key.position(offset + Integer.BYTES);
        key.limit(key.position() + keySize);
        return key.slice();
    }

    private Cell cellAt(final int i) {
        assert 0 <= i && i < rows;
        int offset = offsets.get(i);

        //Key
        final int keySize = cells.getInt(offset);
        offset += Integer.BYTES;
        final ByteBuffer key = cells.duplicate();
        key.position(offset);
        key.limit(key.position() + keySize);
        offset += keySize;

        //Timestamp
        final long timestamp = cells.getLong(offset);
        offset += Long.BYTES;
        if (timestamp < 0) {
            return new Cell(key.slice(), new Value(-timestamp, null));
        } else {
            final int valueSize = cells.getInt(offset);
            offset += Integer.BYTES;
            final ByteBuffer value = cells.duplicate();
            value.position(offset);
            value.limit(value.position() + valueSize)
                    .position(offset)
                    .limit(offset + valueSize);
            return new Cell(key.slice(), new Value(timestamp, value.slice()));
        }
    }

    private int position(final ByteBuffer from) {
        int left = 0;
        int right = rows - 1;
        while (left <= right) {
            final int mid = left + (right - left) / 2;
            final int cmp = keyAt(mid).compareTo(from);
            if (cmp < 0) {
                left = mid + 1;
            } else if (cmp > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return new Iterator<>() {
            int next = position(from);

            @Override
            public boolean hasNext() {
                return next < rows;
            }

            @Override
            public Cell next() {
                assert hasNext();
                return cellAt(next++);
            }
        };
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException("");
    }

}
