package ru.mail.polis.brainlux;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

public final class LSMDao implements DAO {
    private static final String SUFFIX = ".db";
    private static final String TEMP = ".tmp";
    private static final String PREFIX = "SSTABLE";
    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    private final long flushThreshold;
    private final File base;
    private final Collection<SSTable> ssTables;
    private Table memTable;
    private int generation;

    /**
     * Creates persistence LSMDao.
     *
     * @param base           folder with SSTables
     * @param flushThreshold threshold memTable's size
     * @throws IOException if an I/O error occurred
     */

    public LSMDao(
            final File base,
            final long flushThreshold) throws IOException {
        this.base = base;
        assert flushThreshold >= 0L;
        this.flushThreshold = flushThreshold;
        memTable = new MemTable();
        ssTables = new ArrayList<>();
        Files.walkFileTree(base.toPath(), EnumSet.of(FileVisitOption.FOLLOW_LINKS), 1, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path path, final BasicFileAttributes attrs) throws IOException {
                if (path.getFileName().toString().endsWith(SUFFIX)
                        && path.getFileName().toString().startsWith(PREFIX)) {
                    ssTables.add(new SSTable(path.toFile()));
                }
                return FileVisitResult.CONTINUE;
            }
        });
        generation = ssTables.size() - 1;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final Collection<Iterator<Cell>> filesIterators = new ArrayList<>();

        //SSTables iterators
        for (final SSTable ssTable : ssTables) {
            filesIterators.add(ssTable.iterator(from));
        }

        //MemTable iterator
        filesIterators.add(memTable.iterator(from));
        final Iterator<Cell> cells = Iters.collapseEquals(Iterators.mergeSorted(filesIterators, Cell.COMPARATOR),
                Cell::getKey);
        final Iterator<Cell> alive =
                Iterators.filter(
                        cells,
                        cell -> {
                            assert cell != null;
                            return !cell.getValue().isRemoved();
                        });
        return Iterators.transform(
                alive,
                cell -> {
                    assert cell != null;
                    return Record.of(cell.getKey(), cell.getValue().getData());
                });
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.sizeInBytes() >= flushThreshold) {
            flush();
        }
    }


    private void flush() throws IOException {
        final File tmp = new File(base, PREFIX + generation + TEMP);
        SSTable.write(memTable.iterator(EMPTY), tmp);
        final File dest = new File(base, PREFIX + generation + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        generation++;
        memTable = new MemTable();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.sizeInBytes() >= flushThreshold) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
