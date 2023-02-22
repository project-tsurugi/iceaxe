package com.tsurugidb.iceaxe.session.event.logging.file;

import java.nio.file.Path;

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
    private boolean autoFlush = false;
    private int writeExplain = EXPLAIN_FILE;
    private boolean writeReadRecord = false;

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

    @Override
    public String toString() {
        return "TsurugiSessionTxFileLogConfig[outputDir=" + outputDir + ", subDirType=" + subDirType + ", autoFlush=" + autoFlush + ", writeExplain=" + writeExplain + ", writeReadRecord="
                + writeReadRecord + "]";
    }
}
