/*
 * Copyright 2023-2026 Project Tsurugi.
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
package com.tsurugidb.iceaxe.util.io;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.Test;

class StringBuilderWriterTest {

    @Test
    void write() throws IOException {
        try (var target = new StringBuilderWriter(32)) {
            target.write('A');
            target.write(new char[] { 'a', 'b', 'c' });
            target.write(new char[] { '1', '2', '3', '4', '5' }, 1, 3);
            target.write("xyz");

            var buffer = target.getBuffer();
            assertEquals("Aabc234xyz", buffer.toString());
        }
    }

    @Test
    void append() throws IOException {
        try (var target = new StringBuilderWriter(32)) {
            target.append('A');
            target.append("abc");
            target.append("12345", 1, 1 + 3);

            var buffer = target.getBuffer();
            assertEquals("Aabc234", buffer.toString());
        }
    }
}
