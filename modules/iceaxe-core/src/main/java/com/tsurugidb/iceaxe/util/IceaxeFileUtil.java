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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Objects;

import com.tsurugidb.iceaxe.util.io.StringBuilderWriter;

/**
 * Iceaxe file utility.
 *
 * @since 1.8.0
 */
@IceaxeInternal
public final class IceaxeFileUtil {

    private IceaxeFileUtil() {
        // don't instantiate
    }

    /**
     * Write an {@linkplain java.io.InputStream InputStream} to a file.
     *
     * @param path    the path to the file
     * @param is      the InputStream to be written
     * @param options options specifying how the file is opened
     * @throws IOException if an I/O error occurs writing to or creating the file
     * @since 1.16.0
     */
    public static void write(Path path, InputStream is, CopyOption... options) throws IOException {
        Objects.requireNonNull(is);

        try (is) {
            Files.copy(is, path, options);
        }
    }

    /**
     * Write a {@linkplain java.io.Reader Reader} to a file.
     *
     * @param path    the path to the file
     * @param reader  the Reader to be written
     * @param options options specifying how the file is opened
     * @throws IOException if an I/O error occurs writing to or creating the file
     * @since 1.16.0
     */
    public static void write(Path path, Reader reader, OpenOption... options) throws IOException {
        Objects.requireNonNull(reader);

        try (reader; var writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8, options)) {
            reader.transferTo(writer);
        }
    }

    /**
     * Write a {@linkplain java.lang.CharSequence CharSequence} to a file.
     *
     * @param path    the path to the file
     * @param csq     the CharSequence to be written
     * @param options options specifying how the file is opened
     * @return the path
     * @throws IOException if an I/O error occurs writing to or creating the file
     */
    public static Path write(Path path, CharSequence csq, OpenOption... options) throws IOException {
        Objects.requireNonNull(csq);

        byte[] bytes = csq.toString().getBytes(StandardCharsets.UTF_8);
        return Files.write(path, bytes, options);
    }

    /**
     * Read all bytes from an InputStream.
     *
     * @param is the InputStream to read from
     * @return a byte array containing all the bytes read from the InputStream
     * @throws IOException if an I/O error occurs reading from the InputStream
     * @since 1.16.0
     */
    public static byte[] readAllBytes(InputStream is) throws IOException {
        Objects.requireNonNull(is);

        try (is) {
            return is.readAllBytes();
        }
    }

    /**
     * Read a file and return the content as a String.
     *
     * @param path the path to the file
     * @return a String containing the content read from the file
     * @throws IOException if an I/O error occurs reading from the file
     * @since 1.16.0
     */
    public static String readString(Path path) throws IOException {
        Objects.requireNonNull(path);

        return Files.readString(path, StandardCharsets.UTF_8);
    }

    /**
     * Read from Reader and return the content as a String.
     *
     * @param reader the Reader to read from
     * @return the content read from the Reader as a String
     * @throws IOException if an I/O error occurs reading from the file
     * @since 1.16.0
     */
    public static String readString(Reader reader) throws IOException {
        Objects.requireNonNull(reader);

        try (reader; var writer = new StringBuilderWriter(1024)) {
            reader.transferTo(writer);

            return writer.getBuffer().toString();
        }
    }
}
