package com.tsurugidb.iceaxe.sql.result;

import java.io.IOException;

import javax.annotation.OverridingMethodsMustInvokeSuper;

import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi SQL Result.
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
     */
    @IceaxeInternal
    public TsurugiSqlResult(int sqlExecuteId, TsurugiTransaction transaction, TsurugiSql ps, Object parameter) {
        this.iceaxeSqlExecuteId = sqlExecuteId;
        this.ownerTransaction = transaction;
        this.sqlStatement = ps;
        this.sqlParameter = parameter;
    }

    /**
     * initialize.
     * <p>
     * call from constructor after applyCloseTimeout()
     * </p>
     *
     * @param future future to close when an error occurs
     * @throws IOException if transaction already closed
     */
    protected void initialize(FutureResponse<?> future) throws IOException {
        try {
            ownerTransaction.addChild(this);
        } catch (Throwable e) {
            var log = LoggerFactory.getLogger(getClass());
            log.trace("sqlResult.initialize close start", e);
            try {
                IceaxeIoUtil.closeInTransaction(future);
            } catch (Throwable c) {
                e.addSuppressed(c);
            }
            log.trace("sqlResult.initialize close on error end");
            throw e;
        }
    }

    /**
     * get iceaxe SQL executeId.
     *
     * @return iceaxe SQL executeId
     */
    public int getIceaxeSqlExecuteId() {
        return this.iceaxeSqlExecuteId;
    }

    /**
     * fill information in exception.
     *
     * @param exception exception
     * @return exception
     */
    @IceaxeInternal
    public TsurugiTransactionException fillToTsurugiException(TsurugiTransactionException exception) {
        exception.setSql(ownerTransaction, sqlStatement, sqlParameter, this);
        return exception;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() throws IOException, InterruptedException, TsurugiTransactionException {
        ownerTransaction.removeChild(this);
    }
}
