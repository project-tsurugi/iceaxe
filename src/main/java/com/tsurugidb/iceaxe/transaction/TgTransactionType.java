package com.tsurugidb.iceaxe.transaction;

import com.nautilus_technologies.tsubakuro.protos.RequestProtos.TransactionOption.TransactionType;

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
    OCC(TransactionType.TRANSACTION_TYPE_SHORT),
    /**
     * long transaction(read-write)
     */
    BATCH_READ_WRITE(TransactionType.TRANSACTION_TYPE_LONG),
    /**
     * long transaction(read only)
     */
    BATCH_READ_ONLY(TransactionType.TRANSACTION_TYPE_READ_ONLY);

    private final TransactionType lowTransactionType;

    private TgTransactionType(TransactionType lowType) {
        this.lowTransactionType = lowType;
    }

    // internal
    public TransactionType getLowTransactionType() {
        return this.lowTransactionType;
    }
}
