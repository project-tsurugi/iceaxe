package com.tsurugi.iceaxe.transaction;

import com.nautilus_technologies.tsubakuro.protos.RequestProtos.TransactionOption;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.TransactionOption.TransactionType;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.TransactionOption.WritePreserve;

/**
 * Tsurugi Transaction Option
 */
public class TgTransactionOption {

    /**
     * create transaction option
     * 
     * @return transaction option
     */
    public static TgTransactionOption of() {
        return new TgTransactionOption();
    }

    /**
     * create transaction option
     * 
     * @param type transaction type
     * @return transaction option
     */
    public static TgTransactionOption of(TgTransactionType type) {
        return new TgTransactionOption().type(type);
    }

    private final TransactionOption.Builder lowBuilder = TransactionOption.newBuilder().setType(TransactionType.TRANSACTION_TYPE_UNSPECIFIED);
    private TransactionOption lowTransactionOption;

    /**
     * Tsurugi Transaction Option
     */
    public TgTransactionOption() {
        // do nothing
    }

    /**
     * set transaction type
     * 
     * @param type transaction type
     * @return this
     */
    public TgTransactionOption type(TgTransactionType type) {
        var lowType = type.getLowTransactionType();
        lowBuilder.setType(lowType);
        this.lowTransactionOption = null;
        return this;
    }

    /**
     * add write preserve
     * 
     * @param name table name
     * @return this
     */
    public TgTransactionOption addWritePreserveTable(String name) {
        var value = WritePreserve.newBuilder().setName(name).build();
        lowBuilder.addWritePreserves(value);
        this.lowTransactionOption = null;
        return this;
    }

    // internal
    public synchronized TransactionOption toLowTransactionOption() {
        if (this.lowTransactionOption == null) {
            this.lowTransactionOption = lowBuilder.build();
        }
        return this.lowTransactionOption;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + lowBuilder + "]";
    }
}
