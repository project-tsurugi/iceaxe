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
package com.tsurugidb.iceaxe.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class IceaxeFileUtilTest {

    @Test
    void writePathInputStream() throws IOException {
        var value = new byte[] { 1, 2, 3, 0 };

        var path = getTempFilePath();
        IceaxeFileUtil.write(path, new ByteArrayInputStream(value));

        var readValue = Files.readAllBytes(path);
        assertArrayEquals(value, readValue);
    }

    @Test
    void writePathReader() throws IOException {
        var value = "test\nstring\r\nvalue";

        var path = getTempFilePath();
        IceaxeFileUtil.write(path, new StringReader(value));

        var readValue = Files.readString(path, StandardCharsets.UTF_8);
        assertEquals(value, readValue);
    }

    @Test
    void writePathCharSequence() throws IOException {
        var value = "test\nstring\r\nvalue";

        var path = getTempFilePath();
        IceaxeFileUtil.write(path, value);

        var readValue = Files.readString(path, StandardCharsets.UTF_8);
        assertEquals(value, readValue);
    }

    @Test
    void readAllBytes() throws IOException {
        var value = new byte[] { 1, 2, 3, 0 };

        var readValue = IceaxeFileUtil.readAllBytes(new ByteArrayInputStream(value));
        assertArrayEquals(value, readValue);
    }

    @Test
    void readStringPath() throws IOException {
        var value = "test\nstring\r\nvalue";

        var path = getTempFilePath();
        Files.write(path, value.getBytes(StandardCharsets.UTF_8));

        var readValue = IceaxeFileUtil.readString(path);
        assertEquals(value, readValue);
    }

    @Test
    void readStringReader() throws IOException {
        var value = "test\nstring\r\nvalue";

        var readValue = IceaxeFileUtil.readString(new StringReader(value));
        assertEquals(value, readValue);
    }

    private static Path getTempFilePath() throws IOException {
        var tmpDir = System.getProperty("java.io.tmpdir");
        var tmpFile = Path.of(tmpDir, "iceaxe-core.IceaxeFileUtilTest" + System.currentTimeMillis() + ".tmp");
        tmpFile.toFile().deleteOnExit();
        return tmpFile;
    }
}
