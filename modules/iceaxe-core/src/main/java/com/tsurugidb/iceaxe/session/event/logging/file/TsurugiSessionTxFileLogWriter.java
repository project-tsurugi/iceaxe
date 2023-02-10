package com.tsurugidb.iceaxe.session.event.logging.file;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link TsurugiSessionTxFileLogger} writer
 */
public class TsurugiSessionTxFileLogWriter implements Closeable {

    private static final DateTimeFormatter FILENAME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss.SSSSSS");
    private static final DateTimeFormatter HEADER_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final Path outputDir;
    private final PrintWriter writer;
    private final Map<Throwable, Boolean> exceptionMap = new ConcurrentHashMap<>();

    /**
     * Creates a new instance.
     *
     * @param startTime       transaction start time
     * @param threadName      thread name
     * @param parentOutputDir output directory
     */
    public TsurugiSessionTxFileLogWriter(ZonedDateTime startTime, String threadName, Path parentOutputDir) {
        String fileName = "tx" + startTime.format(FILENAME_FORMATTER) + "_" + threadName;
        this.outputDir = parentOutputDir.resolve(fileName);
        try {
            Files.createDirectory(outputDir);
            this.writer = new PrintWriter(Files.newBufferedWriter(outputDir.resolve(fileName + ".log"), StandardCharsets.UTF_8), true);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    /**
     * Prints a String.
     *
     * @param format A format string
     * @param args   Arguments referenced by the format specifiers in the formatstring
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
        writer.print(now.format(HEADER_FORMATTER));
        writer.print(' ');
        writer.println(text);
    }

    /**
     * Writes explain to file.
     *
     * @param sqlId    sqlId
     * @param contents explain contents
     * @throws IOException
     */
    public void writeExplain(int sqlId, String contents) throws IOException {
        String fileName = String.format("sql-%d.explain.json", sqlId);
        Files.writeString(outputDir.resolve(fileName), contents, StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        writer.close();
    }
}
