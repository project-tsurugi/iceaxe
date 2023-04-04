package com.tsurugidb.iceaxe.sql.result;

import java.io.IOException;

import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * Tsurugi SQL Result
 */
public abstract class TsurugiSqlResult implements AutoCloseable {

    private final int iceaxeSqlExecuteId;
    private final TsurugiTransaction ownerTransaction;
    private final TsurugiSql sqlStatement;
    private final Object sqlParameter;

    // internal
    public TsurugiSqlResult(int sqlExecuteId, TsurugiTransaction transaction, TsurugiSql ps, Object parameter) throws IOException {
        this.iceaxeSqlExecuteId = sqlExecuteId;
        this.ownerTransaction = transaction;
        this.sqlStatement = ps;
        this.sqlParameter = parameter;
        transaction.addChild(this);
    }

    /**
     * get iceaxe SQL executeId
     *
     * @return iceaxe SQL executeId
     */
    public int getIceaxeSqlExecuteId() {
        return this.iceaxeSqlExecuteId;
    }

    // internal
    public TsurugiTransactionException fillTsurugiException(TsurugiTransactionException e) {
        e.setSql(ownerTransaction, sqlStatement, sqlParameter, this);
        return e;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() throws IOException, TsurugiTransactionException {
        ownerTransaction.removeChild(this);
    }
}
