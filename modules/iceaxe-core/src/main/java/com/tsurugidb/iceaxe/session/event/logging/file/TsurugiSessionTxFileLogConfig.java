package com.tsurugidb.iceaxe.session.event.logging.file;

import java.nio.file.Path;
import java.time.format.DateTimeFormatter;

/**
 * {@link TsurugiSessionTxFileLogger} config.
 */
public class TsurugiSessionTxFileLogConfig {

    /**
     * Creates a new instance.
     *
     * @param outputDir output directory
     * @return config
     */
    public static TsurugiSessionTxFileLogConfig of(Path outputDir) {
        return new TsurugiSessionTxFileLogConfig(outputDir);
    }

    /** not output explain */
    public static final int EXPLAIN_NOTHING = 0;
    /** output explain to log */
    public static final int EXPLAIN_LOG = 1;
    /** output explain to file */
    public static final int EXPLAIN_FILE = 2;
    /** output explain to log & file */
    public static final int EXPLAIN_BOTH = EXPLAIN_LOG | EXPLAIN_FILE;

    /**
     * sub directory type.
     */
    public enum TgTxFileLogSubDirType {
        NOTHING, TM, TX, TM_TX;
    }

    private final Path outputDir;
    private TgTxFileLogSubDirType subDirType = TgTxFileLogSubDirType.TX;
    private boolean writeSqlFile = false;
    private DateTimeFormatter headerFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private int sqlMaxLength = -1; // <0: 全て出力する, ==0: 出力しない
    private int argMaxLength = -1; // <0: 全て出力する, ==0: 出力しない
    private int writeExplain = EXPLAIN_FILE;
    private boolean writeReadRecord = false;
    private boolean autoFlush = false;

    /**
     * Creates a new instance.
     *
     * @param outputDir output directory
     */
    public TsurugiSessionTxFileLogConfig(Path outputDir) {
        this.outputDir = outputDir;
    }

    /**
     * get output directory
     *
     * @return output directory
     */
    public Path outputDir() {
        return this.outputDir;
    }

    /**
     * set sub directory type
     *
     * @param subDirType sub directory type
     * @return this
     */
    public TsurugiSessionTxFileLogConfig subDirType(TgTxFileLogSubDirType subDirType) {
        this.subDirType = subDirType;
        return this;
    }

    /**
     * get sub directory type
     *
     * @return sub directory type
     */
    public TgTxFileLogSubDirType subDirType() {
        return this.subDirType;
    }

    /**
     * set write SQL file
     *
     * @param write write SQL file
     * @return this
     */
    public TsurugiSessionTxFileLogConfig writeSqlFile(boolean write) {
        this.writeSqlFile = write;
        return this;
    }

    /**
     * get write SQL file
     *
     * @return write SQL file
     */
    public boolean writeSqlFile() {
        return this.writeSqlFile;
    }

    /**
     * set header formatter
     *
     * @param formatter header formatter
     * @return this
     */
    public TsurugiSessionTxFileLogConfig headerFormatter(DateTimeFormatter formatter) {
        this.headerFormatter = formatter;
        return this;
    }

    /**
     * get header formatter
     *
     * @return header formatter
     */
    public DateTimeFormatter headerFormatter() {
        return this.headerFormatter;
    }

    /**
     * set SQL max length
     *
     * @param maxLength SQL max length
     * @return this
     */
    public TsurugiSessionTxFileLogConfig sqlMaxLength(int maxLength) {
        this.sqlMaxLength = maxLength;
        return this;
    }

    /**
     * get SQL max length
     *
     * @return SQL max length
     */
    public int sqlMaxLength() {
        return this.sqlMaxLength;
    }

    /**
     * set SQL argument max length
     *
     * @param maxLength SQL argument max length
     * @return this
     */
    public TsurugiSessionTxFileLogConfig argMaxLength(int maxLength) {
        this.argMaxLength = maxLength;
        return this;
    }

    /**
     * get SQL argument max length
     *
     * @return SQL argument max length
     */
    public int argMaxLength() {
        return this.argMaxLength;
    }

    /**
     * set write explain
     *
     * @param writeExplain write explain
     * @return this
     * @see #EXPLAIN_FILE
     */
    public TsurugiSessionTxFileLogConfig writeExplain(int writeExplain) {
        this.writeExplain = writeExplain;
        return this;
    }

    /**
     * get write explain
     *
     * @return write explain
     * @see #EXPLAIN_FILE
     */
    public int writeExplain() {
        return this.writeExplain;
    }

    /**
     * set write read record
     *
     * @param writeReadRecord write read record
     * @return this
     */
    public TsurugiSessionTxFileLogConfig writeReadRecord(boolean writeReadRecord) {
        this.writeReadRecord = writeReadRecord;
        return this;
    }

    /**
     * get write read record
     *
     * @return write read record
     */
    public boolean writeReadRecord() {
        return this.writeReadRecord;
    }

    /**
     * set auto flush
     *
     * @param autoFlush auto flush
     * @return this
     */
    public TsurugiSessionTxFileLogConfig autoFlush(boolean autoFlush) {
        this.autoFlush = autoFlush;
        return this;
    }

    /**
     * get auto flush
     *
     * @return auto flush
     */
    public boolean autoFlush() {
        return this.autoFlush;
    }

    @Override
    public String toString() {
        return "TsurugiSessionTxFileLogConfig [outputDir=" + outputDir + ", subDirType=" + subDirType + ", writeSqlFile=" + writeSqlFile + ", headerFormatter=" + headerFormatter + ", sqlMaxLength="
                + sqlMaxLength + ", argMaxLength=" + argMaxLength + ", writeExplain=" + writeExplain + ", writeReadRecord=" + writeReadRecord + ", autoFlush=" + autoFlush + "]";
    }
}
