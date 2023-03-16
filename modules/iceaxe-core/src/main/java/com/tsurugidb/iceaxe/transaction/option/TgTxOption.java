package com.tsurugidb.iceaxe.transaction.option;

import java.util.Collection;
import java.util.stream.Stream;

import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;

/**
 * Tsurugi Transaction Option
 */
public interface TgTxOption extends Cloneable {

    /**
     * create transaction option for OCC
     *
     * @return transaction option
     */
    public static TgTxOptionOcc ofOCC() {
        return new TgTxOptionOcc();
    }

    /**
     * create transaction option for long transaction
     *
     * @param writePreserveTableNames table name to Write Preserve
     * @return transaction option
     */
    public static TgTxOptionLtx ofLTX(String... writePreserveTableNames) {
        // return ofLTX(List.of(writePreserveTableNames));
        var txOption = new TgTxOptionLtx();
        for (var name : writePreserveTableNames) {
            txOption.addWritePreserve(name);
        }
        return txOption;
    }

    /**
     * create transaction option for long transaction
     *
     * @param writePreserveTableNames table name to Write Preserve
     * @return transaction option
     */
    public static TgTxOptionLtx ofLTX(Collection<String> writePreserveTableNames) {
        var txOption = new TgTxOptionLtx();
        for (var name : writePreserveTableNames) {
            txOption.addWritePreserve(name);
        }
        return txOption;
    }

    /**
     * create transaction option for long transaction
     *
     * @param writePreserveTableNames table name to Write Preserve
     * @return transaction option
     */
    public static TgTxOptionLtx ofLTX(Stream<String> writePreserveTableNames) {
        var txOption = new TgTxOptionLtx();
        writePreserveTableNames.forEachOrdered(txOption::addWritePreserve);
        return txOption;
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

    // internal
    public TransactionOption toLowTransactionOption();
}
