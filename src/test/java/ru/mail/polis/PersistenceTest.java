/*
 * Copyright 2018 (c) Vadim Tsesko <incubos@yandex.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Persistence tests for {@link DAO} implementations.
 *
 * @author Vadim Tsesko
 */
class PersistenceTest extends TestBase {
    @Test
    void fs(@TempDir File data) throws IOException {
        // Reference key
        final ByteBuffer key = randomKey();

        // Create, fill and remove storage
        try {
            try (DAO dao = DAOFactory.create(data)) {
                dao.upsert(key, randomValue());
            }
        } finally {
            Files.recursiveDelete(data);
        }

        // Check that the storage is empty
        assertFalse(data.exists());
        assertTrue(data.mkdir());
        try (DAO dao = DAOFactory.create(data)) {
            assertThrows(NoSuchElementException.class, () -> dao.get(key));
        }
    }

    @Test
    void reopen(@TempDir File data) throws IOException {
        // Reference value
        final ByteBuffer key = randomKey();
        final ByteBuffer value = randomValue();

        // Create, fill and close storage
        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value);
        }

        // Recreate dao
        try (DAO dao = DAOFactory.create(data)) {
            assertEquals(value, dao.get(key));
        }
    }

    @Test
    void remove(@TempDir File data) throws IOException {
        // Reference value
        final ByteBuffer key = randomKey();
        final ByteBuffer value = randomValue();

        // Create dao and fill data
        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value);
        }

        // Load data and check
        try (DAO dao = DAOFactory.create(data)) {
            assertEquals(value, dao.get(key));

            // Remove data and flush
            dao.remove(key);
        }

        // Load and check not found
        try (DAO dao = DAOFactory.create(data)) {
            assertThrows(NoSuchElementException.class, () -> dao.get(key));
        }
    }

    @RepeatedTest(1000)
    void replaceWithClose(@TempDir File data) throws Exception {
        final ByteBuffer key = randomKey();
        final ByteBuffer value = randomValue();
        final ByteBuffer value2 = randomValue();

        // Initial insert
        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value);
            assertEquals(value, dao.get(key));
        }

        // Reopen
        try (DAO dao = DAOFactory.create(data)) {
            // Check and replace
            assertEquals(value, dao.get(key));
            dao.upsert(key, value2);
            assertEquals(value2, dao.get(key));
        }

        // Reopen
        try (DAO dao = DAOFactory.create(data)) {
            // Last value should win
            assertEquals(value2, dao.get(key));
        }
    }

    @Test
    void flush(@TempDir File data) throws IOException {
        // Reference value
        final int valueSize = 1024 * 1024;
        final ByteBuffer value = randomBuffer(valueSize);
        final int values = (int) (DAOFactory.MAX_HEAP / valueSize + 1);
        final Collection<ByteBuffer> keys = new ArrayList<>(values);

        // Create, fill and close storage
        try (DAO dao = DAOFactory.create(data)) {
            for (int i = 0; i < values; i++) {
                final ByteBuffer key = randomKey();
                keys.add(key);
                dao.upsert(key, join(key, value));
            }
        }

        // Recreate dao and check contents
        try (DAO dao = DAOFactory.create(data)) {
            for (final ByteBuffer key : keys) {
                assertEquals(join(key, value), dao.get(key));
            }
        }
    }

    @Test
    void burn(@TempDir File data) throws IOException {
        // Fixed key
        final ByteBuffer key = randomKey();

        // Overwrite key multiple times
        final int overwrites = 100;
        for (int i = 0; i < overwrites; i++) {
            // Overwrite
            final ByteBuffer value = randomValue();
            try (DAO dao = DAOFactory.create(data)) {
                dao.upsert(key, value);
            }

            // Check
            try (DAO dao = DAOFactory.create(data)) {
                assertEquals(value, dao.get(key));
            }
        }
    }
}
