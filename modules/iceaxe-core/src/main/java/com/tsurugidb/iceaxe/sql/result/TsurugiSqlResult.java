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

    private static boolean defaultEnableCheckResultOnClose = true; // TODO デフォルトをfalseにしたい

    /**
     * set default enable check result on close.
     *
     * @param enabled {@code true}: check result on close
     * @since X.X.X
     */
    public static void setDefaultEnableCheckResultOnClose(boolean enabled) {
        defaultEnableCheckResultOnClose = enabled;
    }

    /**
     * get default enable check result on close.
     *
     * @return {@code true}: check result on close
     * @since X.X.X
     */
    public static boolean getDefaultEnableCheckResultOnClose() {
        return defaultEnableCheckResultOnClose;
    }

    private final int iceaxeSqlExecuteId;
    private final TsurugiTransaction ownerTransaction;
    private final TsurugiSql sqlStatement;
    private final Object sqlParameter;

    /** connect timeout. */
    protected final IceaxeTimeout connectTimeout;
    /** close timeout. */
    protected final IceaxeTimeout closeTimeout;

    private Boolean enableCheckResultOnClose = null;

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
     * @since 1.5.0
     */
    public void setConnectTimeout(long time, TimeUnit unit) {
        setConnectTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set connect timeout.
     *
     * @param timeout time
     * @since 1.5.0
     */
    public void setConnectTimeout(TgTimeValue timeout) {
        connectTimeout.set(timeout);
    }

    /**
     * set close timeout.
     *
     * @param time timeout time
     * @param unit timeout unit
     * @since 1.5.0
     */
    public void setCloseTimeout(long time, TimeUnit unit) {
        setCloseTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set close timeout.
     *
     * @param timeout time
     * @since 1.5.0
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

    /**
     * set enable check result on close.
     *
     * @param enabled {@code true}: check result on close
     * @since X.X.X
     */
    public void setEnableCheckResultOnClose(boolean enabled) {
        this.enableCheckResultOnClose = enabled;
    }

    /**
     * get enable check result on close.
     *
     * @return {@code true}: check result on close
     * @since X.X.X
     */
    protected boolean enableCheckResultOnClose() {
        Boolean enabled = this.enableCheckResultOnClose;
        if (enabled != null) {
            return enabled.booleanValue();
        }
        return getDefaultEnableCheckResultOnClose();
    }
}
