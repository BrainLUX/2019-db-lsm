package ru.mail.polis.brainlux;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Value implements Comparable<Value> {

    private final long ts;
    private final ByteBuffer data;


    Value(final long ts, final ByteBuffer data) {
        this.ts = ts;
        this.data = data;
    }

    public static Value of(final ByteBuffer data) {
        return new Value(System.currentTimeMillis(), data.duplicate());
    }

    static Value tombstone() {
        return new Value(System.currentTimeMillis(), null);
    }

    boolean isRemoved() {
        return data == null;
    }

    ByteBuffer getData() {
        if (data == null) {
            throw new IllegalArgumentException("");
        }
        return data.asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return -Long.compare(ts, o.ts);
    }

    long getTimeStamp() {
        return ts;
    }
}