package com.tsurugidb.iceaxe.result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.result.event.TsurugiResultCountEventListener;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi Result Count for PreparedStatement
 */
@NotThreadSafe
public class TsurugiResultCount extends TsurugiResult {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiResultCount.class);

    private FutureResponse<Void> lowResultFuture;
    private final IceaxeTimeout checkTimeout;
    private final IceaxeTimeout closeTimeout;
    private List<TsurugiResultCountEventListener> eventListenerList = null;

    // internal
    public TsurugiResultCount(int sqlExecuteId, TsurugiTransaction transaction, FutureResponse<Void> lowResultFuture) throws IOException {
        super(sqlExecuteId, transaction);
        this.lowResultFuture = lowResultFuture;

        var sessionOption = transaction.getSessionOption();
        this.checkTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.RESULT_CHECK);
        this.closeTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.RESULT_CLOSE);

        applyCloseTimeout();
    }

    private void applyCloseTimeout() {
        closeTimeout.apply(lowResultFuture);
    }

    /**
     * set check-timeout
     *
     * @param timeout time
     */
    public void setCheckTimeout(long time, TimeUnit unit) {
        setCheckTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set check-timeout
     *
     * @param timeout time
     */
    public void setCheckTimeout(TgTimeValue timeout) {
        checkTimeout.set(timeout);
    }

    /**
     * set close-timeout
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setCloseTimeout(long time, TimeUnit unit) {
        setCloseTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set close-timeout
     *
     * @param timeout time
     */
    public void setCloseTimeout(TgTimeValue timeout) {
        closeTimeout.set(timeout);

        applyCloseTimeout();
    }

    /**
     * add event listener
     *
     * @param listener event listener
     * @return this
     */
    public TsurugiResult addEventListener(TsurugiResultCountEventListener listener) {
        if (this.eventListenerList == null) {
            this.eventListenerList = new ArrayList<>();
        }
        eventListenerList.add(listener);
        return this;
    }

    private void event(Throwable occurred, Consumer<TsurugiResultCountEventListener> action) {
        if (this.eventListenerList != null) {
            try {
                for (var listener : eventListenerList) {
                    action.accept(listener);
                }
            } catch (Throwable e) {
                if (occurred != null) {
                    e.addSuppressed(occurred);
                }
                throw e;
            }
        }
    }

    protected final synchronized void checkLowResult() throws IOException, TsurugiTransactionException {
        if (this.lowResultFuture != null) {
            LOG.trace("lowResult get start");
            Throwable occurred = null;
            try {
                IceaxeIoUtil.getAndCloseFutureInTransaction(lowResultFuture, checkTimeout);
            } catch (Throwable e) {
                occurred = e;
                throw e;
            } finally {
                this.lowResultFuture = null;
                applyCloseTimeout();

                var finalOccurred = occurred;
                event(occurred, listener -> listener.endResult(this, finalOccurred));
            }
            LOG.trace("lowResult get end");
        }
    }

    /**
     * get count
     *
     * @return the row count for SQL Data Manipulation Language (DML) statements
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public int getUpdateCount() throws IOException, TsurugiTransactionException {
        checkLowResult();
        // FIXME 更新件数取得
        // 件数を保持し、何度呼ばれても返せるようにする
//      throw new InternalError("not yet implements");
//      System.err.println("not yet implements TsurugiResultCount.getUpdateCount(), now always returns -1");
        return -1;
    }

    @Override
    public void close() throws IOException, TsurugiTransactionException {
        LOG.trace("result close start");

        Throwable occurred = null;
        try {
            checkLowResult();
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            try {
                // not try-finally
                IceaxeIoUtil.closeInTransaction(lowResultFuture);
                super.close();
            } catch (Throwable e) {
                if (occurred != null) {
                    occurred.addSuppressed(e);
                } else {
                    occurred = e;
                    throw e;
                }
            } finally {
                var finalOccurred = occurred;
                event(occurred, listener -> listener.closeResult(this, finalOccurred));
            }
        }

        LOG.trace("result close end");
    }
}
