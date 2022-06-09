package com.tsurugidb.iceaxe.transaction;

import java.util.Collection;

import com.tsurugidb.jogasaki.proto.SqlRequest.TransactionOption;
import com.tsurugidb.jogasaki.proto.SqlRequest.TransactionType;
import com.tsurugidb.jogasaki.proto.SqlRequest.WritePreserve;

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

    /**
     * create transaction option for OCC
     * 
     * @return transaction option
     */
    public static TgTransactionOption ofOCC() {
        return of(TgTransactionType.OCC);
    }

    /**
     * create transaction option for long transaction
     * 
     * @param writePreserveTableNames table name to Write Preserve
     * @return transaction option
     */
    public static TgTransactionOption ofLTX(String... writePreserveTableNames) {
        // return ofLTX(List.of(writePreserveTableNames));
        var option = of(TgTransactionType.BATCH_READ_WRITE);
        for (var name : writePreserveTableNames) {
            option.addWritePreserveTable(name);
        }
        return option;
    }

    /**
     * create transaction option for long transaction
     * 
     * @param writePreserveTableNames table name to Write Preserve
     * @return transaction option
     */
    public static TgTransactionOption ofLTX(Collection<String> writePreserveTableNames) {
        var option = of(TgTransactionType.BATCH_READ_WRITE);
        for (var name : writePreserveTableNames) {
            option.addWritePreserveTable(name);
        }
        return option;
    }

    /**
     * create transaction option for read only transaction
     * 
     * @return transaction option
     */
    public static TgTransactionOption ofRTX() {
        return of(TgTransactionType.BATCH_READ_ONLY);
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
        var value = WritePreserve.newBuilder().setTableName(name).build();
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
