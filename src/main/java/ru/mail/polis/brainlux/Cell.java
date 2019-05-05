package ru.mail.polis.brainlux;

import java.nio.ByteBuffer;
import java.util.Comparator;

public final class Cell {

    public static final Comparator<Cell> COMPARATOR =
            Comparator.comparing(Cell::getKey).thenComparing(Cell::getValue);

    private final ByteBuffer key;
    private final Value value;

    Cell(ByteBuffer key, Value value) {
        this.key = key;
        this.value = value;
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    public Value getValue() {
        return value;
    }
}