package ru.mail.polis;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks ignoring trash files in persistent data directory.
 *
 * @author Dmitry Schitinin
 */
class TrashTest extends TestBase {
    @Test
    void ignoreEmptyTrashFiles(@TempDir File data) throws IOException {
        // Reference value
        final ByteBuffer key = randomKey();
        final ByteBuffer value = randomValue();

        // Create dao and fill data
        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value);
        }

        createTrashFile(data, "trash.txt");
        createTrashFile(data, "trash.dat");
        createTrashFile(data, "trash");
        createTrashFile(data, "trash_0");
        createTrashFile(data, "trash.db");

        // Load and check stored value
        try (DAO dao = DAOFactory.create(data)) {
            assertEquals(value, dao.get(key));
        }
    }

    @Test
    void ignoreNonEmptyTrashFiles(@TempDir File data) throws IOException {
        // Reference value
        final ByteBuffer key = randomKey();
        final ByteBuffer value = randomValue();

        // Create dao and fill data
        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value);
        }

        createTrashFile(data, "trash.txt", randomValue());
        createTrashFile(data, "trash.dat", randomValue());
        createTrashFile(data, "trash", randomValue());
        createTrashFile(data, "trash_0", randomValue());
        createTrashFile(data, "trash.db", randomValue());

        // Load and check stored value
        try (DAO dao = DAOFactory.create(data)) {
            assertEquals(value, dao.get(key));
        }
    }

    private static void createTrashFile(final File dir,
                                        final String name) throws IOException {
        assertTrue(new File(dir, name).createNewFile());
    }

    private static void createTrashFile(final File dir,
                                        final String name,
                                        final ByteBuffer content) throws IOException {
        try (final FileChannel ch =
                     FileChannel.open(
                             Paths.get(dir.getAbsolutePath(), name),
                             StandardOpenOption.CREATE,
                             StandardOpenOption.WRITE)) {
            ch.write(content);
        }
    }
}
