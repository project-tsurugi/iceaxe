package com.tsurugidb.iceaxe.sql.result;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.IceaxeTimeoutCloseable;
import com.tsurugidb.iceaxe.util.TgTimeValue;

/**
 * Tsurugi SQL Result.
 */
public abstract class TsurugiSqlResult implements IceaxeTimeoutCloseable {

    private final int iceaxeSqlExecuteId;
    private final TsurugiTransaction ownerTransaction;
    private final TsurugiSql sqlStatement;
    private final Object sqlParameter;

    protected final IceaxeTimeout connectTimeout;
    protected final IceaxeTimeout closeTimeout;

    /**
     * Creates a new instance.
     * <p>
     * Call {@link #initialize()} after construct.
     * </p>
     *
     * @param sqlExecuteId iceaxe SQL executeId
     * @param transaction  transaction
     * @param ps           SQL definition
     * @param parameter    SQL parameter
     * @param connectKey   connect timeout key
     * @param closeKey     close timeout key
     */
    @IceaxeInternal
    public TsurugiSqlResult(int sqlExecuteId, TsurugiTransaction transaction, TsurugiSql ps, Object parameter, TgTimeoutKey connectKey, TgTimeoutKey closeKey) {
        this.iceaxeSqlExecuteId = sqlExecuteId;
        this.ownerTransaction = transaction;
        this.sqlStatement = ps;
        this.sqlParameter = parameter;

        var sessionOption = transaction.getSessionOption();
        this.connectTimeout = new IceaxeTimeout(sessionOption, connectKey);
        this.closeTimeout = new IceaxeTimeout(sessionOption, closeKey);
    }

    /**
     * initialize.
     * <p>
     * Call this method only once after construct.
     * </p>
     *
     * @throws IOException if transaction already closed
     */
    @IceaxeInternal
    protected void initialize() throws IOException {
        ownerTransaction.addChild(this);
    }

    /**
     * set connect timeout.
     *
     * @param time time value
     * @param unit time unit
     * @since X.X.X
     */
    public void setConnectTimeout(long time, TimeUnit unit) {
        setConnectTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set connect timeout.
     *
     * @param timeout time
     * @since X.X.X
     */
    public void setConnectTimeout(TgTimeValue timeout) {
        connectTimeout.set(timeout);
    }

    /**
     * set close timeout.
     *
     * @param time timeout time
     * @param unit timeout unit
     * @since X.X.X
     */
    public void setCloseTimeout(long time, TimeUnit unit) {
        setCloseTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set close timeout.
     *
     * @param timeout time
     * @since X.X.X
     */
    public void setCloseTimeout(TgTimeValue timeout) {
        closeTimeout.set(timeout);
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
//  @OverridingMethodsMustInvokeSuper
    public void close() throws IOException, InterruptedException, TsurugiTransactionException {
        ownerTransaction.removeChild(this);
    }
}
