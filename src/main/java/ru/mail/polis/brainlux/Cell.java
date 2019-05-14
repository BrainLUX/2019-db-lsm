package ru.mail.polis.brainlux;

import java.nio.ByteBuffer;
import java.util.Comparator;

public final class Cell {
    static final Comparator<Cell> COMPARATOR =
            Comparator.comparing(Cell::getKey).thenComparing(Cell::getValue).thenComparing(Cell::getGeneration);

    private final ByteBuffer key;
    private final Value value;
    private final int generation;

    Cell(final ByteBuffer key, final Value value, final int generation) {
        this.key = key;
        this.value = value;
        this.generation = generation;
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    public Value getValue() {
        return value;
    }

    private int getGeneration() {
        return generation;
    }
    
}
