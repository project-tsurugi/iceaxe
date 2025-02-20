/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.iceaxe.session.event.logging.file;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.tsurugidb.iceaxe.util.IceaxeFileUtil;

/**
 * {@link TsurugiSessionTxFileLogger} writer.
 */
public class TsurugiSessionTxFileLogWriter implements Closeable {

    /** transaction file log config */
    protected final TsurugiSessionTxFileLogConfig config;
    private final Path outputDir;
    private final PrintWriter writer;
    private final Map<Throwable, Boolean> exceptionMap = new ConcurrentHashMap<>();

    /**
     * Creates a new instance.
     *
     * @param config    transaction file log config
     * @param outputDir output directory path
     * @param writer    print writer
     */
    public TsurugiSessionTxFileLogWriter(TsurugiSessionTxFileLogConfig config, Path outputDir, PrintWriter writer) {
        this.config = config;
        this.outputDir = outputDir;
        this.writer = writer;
    }

    /**
     * Prints a String.
     *
     * @param format A format string
     * @param args   Arguments referenced by the format specifiers in the format string
     */
    public void println(String format, Object... args) {
        String text = String.format(format, args);
        println(text);
    }

    /**
     * Prints a String.
     *
     * @param t exception
     */
    public void println(Throwable t) {
        if (t != null) {
            if (exceptionMap.putIfAbsent(t, Boolean.TRUE) == null) {
                t.printStackTrace(writer);
            } else {
                writer.println(t.getClass().getName() + ": " + t.getMessage());
            }
        }
    }

    /**
     * Prints a String.
     *
     * @param text the String value to be printed
     */
    public void println(String text) {
        var now = ZonedDateTime.now();
        writer.print(now.format(config.headerFormatter()));
        writer.print(' ');
        writer.println(text);
    }

    /**
     * Writes explain to file.
     *
     * @param sqlId    sqlId
     * @param contents explain contents
     * @throws IOException if an I/O error occurs writing to or creating the file
     */
    public void writeExplain(int sqlId, String contents) throws IOException {
        String fileName = String.format("sql-%d.explain.json", sqlId);
        IceaxeFileUtil.writeString(outputDir.resolve(fileName), contents);
    }

    @Override
    public void close() {
        writer.close();
    }
}
