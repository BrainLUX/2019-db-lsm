package ru.mail.polis.brainlux;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Value implements Comparable<Value> {

    private final long ts;
    private final ByteBuffer data;

    public Value(final long ts, final ByteBuffer data) {
        assert (ts >= 0);
        this.ts = ts;
        this.data = data;
    }

    public static Value of(final ByteBuffer data) {
        return new Value(System.currentTimeMillis(), data.duplicate());
    }

    public static Value tombstone() {
        return new Value(System.currentTimeMillis(), null);
    }

    public boolean isRemoved() {
        return data == null;
    }

    public ByteBuffer getData() {
        if (data == null) {
            throw new IllegalArgumentException("");
        }
        return data.asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull Value o) {
        return -Long.compare(ts, o.ts);
    }

    public long getTimeStamp() {
        return ts;
    }
}