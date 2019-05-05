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

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Contains utility methods for unit tests.
 *
 * @author incubos
 */
abstract class TestBase {
    static final int KEY_LENGTH = 16;
    private static final int VALUE_LENGTH = 1024;

    @NotNull
    static ByteBuffer randomBuffer(final int length) {
        assert length > 0;
        final byte[] result = new byte[length];
        ThreadLocalRandom.current().nextBytes(result);
        return ByteBuffer.wrap(result);
    }

    @NotNull
    static ByteBuffer randomKey() {
        return randomBuffer(KEY_LENGTH);
    }

    @NotNull
    static ByteBuffer randomValue() {
        return randomBuffer(VALUE_LENGTH);
    }

    @NotNull
    static ByteBuffer join(
            @NotNull final ByteBuffer left,
            @NotNull final ByteBuffer right) {
        final ByteBuffer result = ByteBuffer.allocate(left.remaining() + right.remaining());
        result.put(left.duplicate());
        result.put(right.duplicate());
        result.rewind();
        return result;
    }
}
