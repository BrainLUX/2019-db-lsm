package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

public class DAOImpl implements DAO {

    private final NavigableMap<ByteBuffer, Record> base = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) {
        return base.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) {
        base.put(key, Record.of(key, value));
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        base.remove(key);
    }

    @Override
    public void close() {
    }
}
