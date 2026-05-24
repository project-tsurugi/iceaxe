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

import java.io.IOException;
import java.io.Writer;
import java.util.Objects;

/**
 * A Writer that writes to a StringBuilder.
 *
 * @since 0.16.0
 */
public class StringBuilderWriter extends Writer {

    private final StringBuilder buffer;

    /**
     * Creates a new instance.
     *
     * @param capacity initial capacity
     */
    public StringBuilderWriter(int capacity) {
        this.buffer = new StringBuilder(capacity);
    }

    @Override
    public void write(int c) throws IOException {
        buffer.append((char) c);
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        Objects.requireNonNull(cbuf);
        buffer.append(cbuf, off, len);
    }

    @Override
    public void write(String str) throws IOException {
        Objects.requireNonNull(str);
        buffer.append(str);
    }

    @Override
    public void write(String str, int off, int len) throws IOException {
        Objects.requireNonNull(str);
        buffer.append(str, off, off + len);
    }

    @Override
    public Writer append(CharSequence csq) throws IOException {
        buffer.append(csq);
        return this;
    }

    @Override
    public Writer append(CharSequence csq, int start, int end) throws IOException {
        buffer.append(csq, start, end);
        return this;
    }

    @Override
    public Writer append(char c) throws IOException {
        buffer.append(c);
        return this;
    }

    @Override
    public void flush() throws IOException {
        // do nothing
    }

    /**
     * Returns the underlying StringBuilder.
     *
     * @return the underlying StringBuilder
     */
    public StringBuilder getBuffer() {
        return buffer;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
