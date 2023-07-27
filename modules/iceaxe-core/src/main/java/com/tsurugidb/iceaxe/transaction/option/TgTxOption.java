package com.tsurugidb.iceaxe.transaction.option;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;

/**
 * Tsurugi Transaction Option
 */
public interface TgTxOption extends Cloneable {

    /**
     * create transaction option for short transaction
     *
     * @return transaction option
     */
    public static TgTxOptionOcc ofOCC() {
        return new TgTxOptionOcc();
    }

    /**
     * create transaction option for short transaction
     *
     * @param txOption source transaction option
     * @return transaction option
     */
    public static TgTxOptionOcc ofOCC(@Nonnull TgTxOption txOption) {
        Objects.requireNonNull(txOption);
        return new TgTxOptionOcc().fillFrom(txOption);
    }

    /**
     * create transaction option for long transaction
     *
     * @param writePreserveTableNames table name to Write Preserve
     * @return transaction option
     */
    public static TgTxOptionLtx ofLTX(String... writePreserveTableNames) {
        return new TgTxOptionLtx().addWritePreserve(writePreserveTableNames);
    }

    /**
     * create transaction option for long transaction
     *
     * @param writePreserveTableNames table name to Write Preserve
     * @return transaction option
     */
    public static TgTxOptionLtx ofLTX(Collection<String> writePreserveTableNames) {
        return new TgTxOptionLtx().addWritePreserve(writePreserveTableNames);
    }

    /**
     * create transaction option for long transaction
     *
     * @param writePreserveTableNames table name to Write Preserve
     * @return transaction option
     */
    public static TgTxOptionLtx ofLTX(Stream<String> writePreserveTableNames) {
        return new TgTxOptionLtx().addWritePreserve(writePreserveTableNames);
    }

    /**
     * create transaction option for long transaction
     *
     * @param txOption source transaction option
     * @return transaction option
     */
    public static TgTxOptionLtx ofLTX(@Nonnull TgTxOption txOption) {
        Objects.requireNonNull(txOption);
        return new TgTxOptionLtx().fillFrom(txOption);
    }

    /**
     * create transaction option for read only transaction
     *
     * @return transaction option
     */
    public static TgTxOptionRtx ofRTX() {
        return new TgTxOptionRtx();
    }

    /**
     * create transaction option for read only transaction
     *
     * @param txOption source transaction option
     * @return transaction option
     */
    public static TgTxOptionRtx ofRTX(@Nonnull TgTxOption txOption) {
        Objects.requireNonNull(txOption);
        return new TgTxOptionRtx().fillFrom(txOption);
    }

    /**
     * create transaction option for DDL(LTX)
     *
     * @return transaction option
     */
    public static TgTxOptionLtx ofDDL() {
        return new TgTxOptionLtx().includeDdl(true);
    }

    /**
     * create transaction option for DDL(LTX)
     *
     * @param txOption source transaction option
     * @return transaction option
     */
    public static TgTxOptionLtx ofDDL(@Nonnull TgTxOption txOption) {
        Objects.requireNonNull(txOption);
        return ofLTX(txOption).includeDdl(true);
    }

    /**
     * get transaction type name
     *
     * @return transaction type
     */
    public String typeName();

    /**
     * get transaction type
     *
     * @return transaction type
     */
    public TransactionType type();

    /**
     * check transaction type is OCC
     *
     * @return {@code true} if OCC
     */
    public default boolean isOCC() {
        return type() == TransactionType.SHORT;
    }

    /**
     * check transaction type is LTX
     *
     * @return {@code true} if LTX
     */
    public default boolean isLTX() {
        return type() == TransactionType.LONG;
    }

    /**
     * check transaction type is RTX
     *
     * @return {@code true} if RTX
     */
    public default boolean isRTX() {
        return type() == TransactionType.READ_ONLY;
    }

    /**
     * set label
     *
     * @param label label
     * @return this
     */
    public TgTxOption label(String label);

    /**
     * get label
     *
     * @return label
     */
    public String label();

    /**
     * clone transaction option
     *
     * @return new transaction option
     */
    public TgTxOption clone();

    /**
     * clone transaction option
     *
     * @param label label
     * @return new transaction option
     */
    public TgTxOption clone(String label);

    /**
     * convert to {@link TransactionOption}
     *
     * @return transaction option
     */
    @IceaxeInternal
    public TransactionOption toLowTransactionOption();
}
