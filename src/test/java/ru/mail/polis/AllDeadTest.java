package ru.mail.polis;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opentest4j.AssertionFailedError;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Generates a lot of tombstones
 * and ensures that resulted DAO is empty.
 *
 * @author Dmitry Schitinin
 */
class AllDeadTest extends TestBase {

    private static final int TOMBSTONES_COUNT = 1000000;

    @Test
    void deadAll(@TempDir File data) throws IOException {
        // Create, fill, read and remove
        try (DAO dao = DAOFactory.create(data)) {
            final Iterator<ByteBuffer> tombstones =
                    Stream.generate(TestBase::randomKey)
                            .limit(TOMBSTONES_COUNT)
                            .iterator();
            while (tombstones.hasNext()) {
                try {
                    dao.remove(tombstones.next());
                } catch (IOException e) {
                    throw new AssertionFailedError("Unable to remove");
                }
            }

            // Check contents
            final Iterator<Record> empty = dao.iterator(ByteBuffer.allocate(0));
            assertFalse(empty.hasNext());
        }
    }
}
