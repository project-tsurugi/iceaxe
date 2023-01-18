package com.tsurugidb.iceaxe.transaction.option;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;
import com.tsurugidb.sql.proto.SqlRequest.WritePreserve;

/**
 * Tsurugi Transaction Option (LTX)
 */
@ThreadSafe
public class TgTxOptionLtx extends AbstractTgTxOptionLong<TgTxOptionLtx> {

    private List<String> writePreserveList = new ArrayList<>();

    @Override
    public String typeName() {
        return "LTX";
    }

    @Override
    public TransactionType type() {
        return TransactionType.LONG;
    }

    /**
     * add write preserve
     *
     * @param tableName table name
     * @return this
     */
    public synchronized TgTxOptionLtx addWritePreserve(String tableName) {
        writePreserveList.add(tableName);
        reset();
        return this;
    }

    /**
     * get write preserve
     *
     * @return list of table name
     */
    public List<String> writePreserve() {
        return this.writePreserveList;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    protected void initializeLowTransactionOption(TransactionOption.Builder lowBuilder) {
        super.initializeLowTransactionOption(lowBuilder);

        for (String name : writePreserveList) {
            var value = WritePreserve.newBuilder().setTableName(name).build();
            lowBuilder.addWritePreserves(value);
        }
    }

    @Override
    public TgTxOptionLtx clone() {
        var option = super.clone();
        option.writePreserveList = new ArrayList<>(this.writePreserveList);
        return option;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        appendString(sb, "writePreserve", writePreserveList);
    }
}
