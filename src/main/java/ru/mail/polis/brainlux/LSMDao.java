package ru.mail.polis.brainlux;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

public final class LSMDao implements DAO {
    public static final String SUFFIX = ".dat";
    public static final String TEMP = ".tmp";

    private Table memTable;
    private final long flushThreshold;
    private final File base;
    private int generation;
    private final Collection<Path> files;

    public LSMDao(
            final File base,
            final long flushThreshold) throws IOException {
        this.base = base;
        assert flushThreshold >= 0L;
        this.flushThreshold = flushThreshold;
        memTable = new MemTable();
        files = new ArrayList<>();
        Files.walk(base.toPath(), 1).filter(path -> path.getFileName().toString().endsWith(SUFFIX))
                .forEach(files::add);
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        final ArrayList<Iterator<Cell>> filesIterators = new ArrayList<>();

        //SSTables iterators
        for (final Path path : this.files) {
            filesIterators.add(new SSTable(path.toFile()).iterator(from));
        }

        //MemTable iterator
        filesIterators.add(memTable.iterator(from));
        final Iterator<Cell> cells = Iters.collapseEquals(Iterators.mergeSorted(filesIterators, Cell.COMPARATOR)
                , Cell::getKey);
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
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.sizeInBytes() >= flushThreshold) {
            flush();
        }
    }


    private void flush() throws IOException {
        final File tmp = new File(base, generation + TEMP);
        SSTable.write(memTable.iterator(ByteBuffer.allocate(0)), tmp);
        final File dest = new File(base, generation + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        generation++;
        memTable = new MemTable();
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
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
