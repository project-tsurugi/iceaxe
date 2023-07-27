package com.tsurugidb.iceaxe.sql.result;

import java.io.IOException;

import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeInternal;

/**
 * Tsurugi SQL Result
 */
public abstract class TsurugiSqlResult implements AutoCloseable {

    private final int iceaxeSqlExecuteId;
    private final TsurugiTransaction ownerTransaction;
    private final TsurugiSql sqlStatement;
    private final Object sqlParameter;

    /**
     * Creates a new instance.
     *
     * @param sqlExecuteId iceaxe SQL executeId
     * @param transaction  transaction
     * @param ps           SQL definition
     * @param parameter    SQL parameter
     * @throws IOException
     */
    @IceaxeInternal
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

    /**
     * fill information in exception
     *
     * @param e exception
     * @return exception
     */
    @IceaxeInternal
    public TsurugiTransactionException fillToTsurugiException(TsurugiTransactionException e) {
        e.setSql(ownerTransaction, sqlStatement, sqlParameter, this);
        return e;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() throws IOException, InterruptedException, TsurugiTransactionException {
        ownerTransaction.removeChild(this);
    }
}
