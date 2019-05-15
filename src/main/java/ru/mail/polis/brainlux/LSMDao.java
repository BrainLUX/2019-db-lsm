package ru.mail.polis.brainlux;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

public final class LSMDao implements DAO {
    private static final String SUFFIX = ".db";
    private static final String TEMP = ".tmp";
    private static final String PREFIX = "SSTABLE";
    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);
    private static final int TABLESCOUNT = 16;

    private final long flushThreshold;
    private final File base;
    private final Collection<SSTable> ssTables;
    private final Logger log = LoggerFactory.getLogger(LSMDao.class);
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
                    generation = Integer.max(generation, getGeneration(new StringBuffer(path.toString())
                            .reverse().toString()));
                }
                return FileVisitResult.CONTINUE;
            }
        });
        generation++;
    }

    private int getGeneration(@NotNull final String path) {
        final StringBuilder digit = new StringBuilder();
        for (final char c : path.toCharArray()) {
            if (Character.isDigit(c)) {
                digit.append(c);
            } else {
                if (digit.length() > 0) {
                    return Integer.parseInt(digit.reverse().toString());
                }
            }
        }
        return 0;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        return Iterators.transform(
                cellIterator(from),
                cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    @NotNull
    private Iterator<Cell> cellIterator(@NotNull final ByteBuffer from) throws IOException {
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
                        cell -> !cell.getValue().isRemoved());
        return alive;
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.sizeInBytes() >= flushThreshold) {
            flush(memTable.iterator(EMPTY));
        }
        if (ssTables.size() > TABLESCOUNT) {
            compact();
        }
    }

    private String flush(@NotNull final Iterator iterator) throws IOException {
        final File tmp = new File(base, PREFIX + generation + TEMP);
        SSTable.write(iterator, tmp);
        final File dest = new File(base, PREFIX + generation + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        generation++;
        memTable = new MemTable();
        return dest.toPath().toString();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.sizeInBytes() >= flushThreshold) {
            flush(memTable.iterator(EMPTY));
        }
        if (ssTables.size() > TABLESCOUNT) {
            compact();
        }
    }

    @Override
    public void close() throws IOException {
        if (memTable.sizeInBytes() > 0) {
            flush(memTable.iterator(EMPTY));
        }
    }

    @Override
    public void compact() throws IOException {
        final String path = flush(cellIterator(EMPTY));
        ssTables.forEach(ssTable -> {
            try {
                Files.delete(ssTable.getTable().toPath());
            } catch (IOException e) {
                log.error("Can't delete ssTable", e);
            }
        });
        ssTables.clear();
        ssTables.add(new SSTable(new File(path)));
        memTable = new MemTable();
    }

}
