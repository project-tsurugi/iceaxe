package com.tsurugidb.iceaxe.transaction;

import com.tsurugidb.jogasaki.proto.SqlRequest.TransactionType;

/**
 * Tsurugi Transaction Type
 */
public enum TgTransactionType {
    /**
     * unspecified
     */
    UNSPECIFIED(TransactionType.TRANSACTION_TYPE_UNSPECIFIED),
    /**
     * short transaction
     */
    OCC(TransactionType.SHORT),
    /**
     * long transaction(read-write)
     */
    BATCH_READ_WRITE(TransactionType.LONG),
    /**
     * long transaction(read only)
     */
    BATCH_READ_ONLY(TransactionType.READ_ONLY);

    private final TransactionType lowTransactionType;

    private TgTransactionType(TransactionType lowType) {
        this.lowTransactionType = lowType;
    }

    // internal
    public TransactionType getLowTransactionType() {
        return this.lowTransactionType;
    }
}
