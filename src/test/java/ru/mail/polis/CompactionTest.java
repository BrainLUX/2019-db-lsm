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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Compaction tests for {@link DAO} implementations
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
class CompactionTest extends TestBase {
    @Test
    void overwrite(@TempDir File data) throws IOException {
        // Reference value
        final int valueSize = 1024 * 1024;
        final int keyCount = 10;
        final int overwrites = 10;

        final ByteBuffer value = randomBuffer(valueSize);
        final Collection<ByteBuffer> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keys.add(randomKey());
        }

        // Overwrite keys several times each time closing DAO
        for (int round = 0; round < overwrites; round++) {
            try (DAO dao = DAOFactory.create(data)) {
                for (final ByteBuffer key : keys) {
                    dao.upsert(key, join(key, value));
                }
            }
        }

        // Check the contents
        try (DAO dao = DAOFactory.create(data)) {
            for (final ByteBuffer key : keys) {
                assertEquals(join(key, value), dao.get(key));
            }

            // Compact
            dao.compact();

            // Check the contents
            for (final ByteBuffer key : keys) {
                assertEquals(join(key, value), dao.get(key));
            }
        }

        // Check store size
        final long size = Files.directorySize(data);
        final long minSize = keyCount * (KEY_LENGTH + KEY_LENGTH + valueSize);

        // Heuristic
        assertTrue(size > minSize);
        assertTrue(size < 2 * minSize);
    }

    @Test
    void clear(@TempDir File data) throws IOException {
        // Reference value
        final int valueSize = 1024 * 1024;
        final int keyCount = 10;

        final ByteBuffer value = randomBuffer(valueSize);
        final Collection<ByteBuffer> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keys.add(randomKey());
        }

        // Insert keys
        try (DAO dao = DAOFactory.create(data)) {
            for (final ByteBuffer key : keys) {
                dao.upsert(key, join(key, value));
            }
        }

        // Check the contents
        try (DAO dao = DAOFactory.create(data)) {
            for (final ByteBuffer key : keys) {
                assertEquals(join(key, value), dao.get(key));
            }

            // Remove keys
            for (final ByteBuffer key : keys) {
                dao.remove(key);
            }
        }

        // Compact
        try (DAO dao = DAOFactory.create(data)) {
            dao.compact();
        }

        // Check the contents
        try (DAO dao = DAOFactory.create(data)) {
            for (final ByteBuffer key : keys) {
                assertThrows(NoSuchElementException.class, () -> dao.get(key));
            }
        }

        // Check store size
        final long size = Files.directorySize(data);

        // Heuristic
        assertTrue(size < valueSize);
    }

    @Test
    void single(@TempDir File data) throws IOException {
        // Reference value
        final int valueSize = 1024 * 1024;

        final ByteBuffer key = randomKey();
        final ByteBuffer value = randomBuffer(valueSize);

        // Insert single key, assume only one SSTable will be flushed ;)
        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value);
        }

        final long initialSize = Files.directorySize(data);

        // Compact
        try (DAO dao = DAOFactory.create(data)) {
            dao.compact();
        }

        // Check the contents
        try (DAO dao = DAOFactory.create(data)) {
            assertEquals(value, dao.get(key));
        }

        // Check store size
        final long compactedSize = Files.directorySize(data);

        // No removals/updates => no real compaction
        assertEquals(initialSize, compactedSize);
    }

    @Test
    void fileCollision(@TempDir File data) throws IOException {
        // Reference value
        final int keyCount = 10;
        // How many times data will be flushed
        final int portionsCount = 100;
        // On which step compaction will be performed
        final int compactIteration = 2;
        // Last value for each dumped key
        final Map<ByteBuffer, ByteBuffer> entries =
                new HashMap<>(keyCount * portionsCount);

        for (int i = 0; i < portionsCount; i++) {
            Map<ByteBuffer, ByteBuffer> dump = new HashMap<>(keyCount);
            for (int j = 0; j < keyCount; j++) {
                dump.put(TestBase.randomKey(), TestBase.randomValue());
            }
            try (DAO dao = DAOFactory.create(data)) {
                for (final Map.Entry<ByteBuffer, ByteBuffer> entry : dump.entrySet()) {
                    dao.upsert(entry.getKey(), entry.getValue());
                }
            }
            entries.putAll(dump);
            if (i == compactIteration) {
                // Compact
                try (DAO dao = DAOFactory.create(data)) {
                    dao.compact();
                }
            }
        }

        // Check the contents
        try (DAO dao = DAOFactory.create(data)) {
            for (final Map.Entry<ByteBuffer, ByteBuffer> entry : entries.entrySet()) {
                assertEquals(entry.getValue(), dao.get(entry.getKey()));
            }
        }
    }
}
