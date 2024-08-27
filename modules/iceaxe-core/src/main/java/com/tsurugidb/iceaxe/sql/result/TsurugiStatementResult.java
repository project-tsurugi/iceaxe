/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe.sql.result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.sql.result.event.TsurugiStatementResultEventListener;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.tsubakuro.sql.ExecuteResult;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi SQL Result for insert/update/delete.
 *
 * @see TsurugiSqlStatement#execute(TsurugiTransaction)
 * @see TsurugiSqlPreparedStatement#execute(TsurugiTransaction, Object)
 */
@NotThreadSafe
public class TsurugiStatementResult extends TsurugiSqlResult {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiStatementResult.class);

    private FutureResponse<ExecuteResult> lowResultFuture;
    private TgResultCount resultCount = null;
    private List<TsurugiStatementResultEventListener> eventListenerList = null;
    private boolean checkResultOnClose = true;

    /**
     * Creates a new instance.
     * <p>
     * Call {@link #initialize(FutureResponse)} after construct.
     * </p>
     *
     * @param sqlExecuteId iceaxe SQL executeId
     * @param transaction  transaction
     * @param ps           SQL definition
     * @param parameter    SQL parameter
     */
    @IceaxeInternal
    public TsurugiStatementResult(int sqlExecuteId, TsurugiTransaction transaction, TsurugiSql ps, Object parameter) {
        super(sqlExecuteId, transaction, ps, parameter, TgTimeoutKey.RESULT_CONNECT, TgTimeoutKey.RESULT_CLOSE);
    }

    /**
     * initialize.
     * <p>
     * Call this method only once after construct.
     * </p>
     *
     * @param lowResultFuture future of ExecuteResult
     * @throws IOException if transaction already closed
     * @since 1.3.0
     */
    @IceaxeInternal
    public void initialize(FutureResponse<ExecuteResult> lowResultFuture) throws IOException {
        if (this.lowResultFuture != null || this.resultCount != null) {
            throw new IllegalStateException("initialize() is already called");
        }

        this.lowResultFuture = Objects.requireNonNull(lowResultFuture);

        try {
            super.initialize();
        } catch (Throwable e) {
            var log = LoggerFactory.getLogger(getClass());
            log.trace("TsurugiStatementResult.initialize close start", e);
            try {
                IceaxeIoUtil.closeInTransaction(closeTimeout.getNanos(), IceaxeErrorCode.RESULT_CLOSE_TIMEOUT, IceaxeErrorCode.RESULT_CLOSE_ERROR, //
                        lowResultFuture);
            } catch (Throwable c) {
                e.addSuppressed(c);
            }
            this.checkResultOnClose = false;
            log.trace("TsurugiStatementResult.initialize close end");
            throw e;
        }
    }

    /**
     * add event listener.
     *
     * @param listener event listener
     * @return this
     */
    public TsurugiSqlResult addEventListener(TsurugiStatementResultEventListener listener) {
        if (this.eventListenerList == null) {
            this.eventListenerList = new ArrayList<>();
        }
        eventListenerList.add(listener);
        return this;
    }

    /**
     * find event listener.
     *
     * @param predicate predicate for event listener
     * @return event listener
     * @since 1.3.0
     */
    public Optional<TsurugiStatementResultEventListener> findEventListener(Predicate<TsurugiStatementResultEventListener> predicate) {
        var listenerList = this.eventListenerList;
        if (listenerList != null) {
            for (var listener : listenerList) {
                if (predicate.test(listener)) {
                    return Optional.of(listener);
                }
            }
        }
        return Optional.empty();
    }

    private void event(Throwable occurred, Consumer<TsurugiStatementResultEventListener> action) {
        var listenerList = this.eventListenerList;
        if (listenerList != null) {
            try {
                for (var listener : listenerList) {
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

    /**
     * check low result.
     *
     * @throws IOException                 if an I/O error occurs while retrieving result
     * @throws InterruptedException        if interrupted while retrieving result
     * @throws TsurugiTransactionException if server error occurs while retrieving result
     */
    @IceaxeInternal
    public final synchronized void checkLowResult() throws IOException, InterruptedException, TsurugiTransactionException {
        this.checkResultOnClose = false;
        if (this.resultCount == null) {
            if (this.lowResultFuture == null) {
                throw new IllegalStateException("initialize() is not called");
            }

            LOG.trace("lowResult get start");
            try {
                var lowExecuteResult = IceaxeIoUtil.getAndCloseFutureInTransaction(lowResultFuture, //
                        connectTimeout, IceaxeErrorCode.RESULT_CONNECT_TIMEOUT, //
                        IceaxeErrorCode.RESULT_CLOSE_TIMEOUT);
                this.resultCount = new TgResultCount(lowExecuteResult);
            } catch (TsurugiTransactionException e) {
                fillToTsurugiException(e);
                event(e, listener -> listener.endResult(this, e));
                throw e;
            } catch (Throwable e) {
                event(e, listener -> listener.endResult(this, e));
                throw e;
            }
            LOG.trace("lowResult get end");

            this.lowResultFuture = null;

            event(null, listener -> listener.endResult(this, null));
        }
    }

    /**
     * get count.
     *
     * @return the row count for SQL Data Manipulation Language (DML) statements
     * @throws IOException                 if an I/O error occurs while retrieving result
     * @throws InterruptedException        if interrupted while retrieving result
     * @throws TsurugiTransactionException if server error occurs while retrieving result
     * @see #getCountDetail()
     */
    public int getUpdateCount() throws IOException, InterruptedException, TsurugiTransactionException {
        return (int) getCountDetail().getTotalCount();
    }

    /**
     * get count detail.
     *
     * @return the row count for SQL Data Manipulation Language (DML) statements
     * @throws IOException                 if an I/O error occurs while retrieving result
     * @throws InterruptedException        if interrupted while retrieving result
     * @throws TsurugiTransactionException if server error occurs while retrieving result
     * @since 1.1.0
     */
    public TgResultCount getCountDetail() throws IOException, InterruptedException, TsurugiTransactionException {
        checkLowResult();
        if (this.resultCount == null) {
            throw new IllegalStateException("resultCount==null");
        }
        return this.resultCount;
    }

    // close

    @Override
    public void close() throws IOException, InterruptedException, TsurugiTransactionException {
        close(closeTimeout.getNanos());
    }

    @Override
    public void close(long timeoutNanos) throws IOException, InterruptedException, TsurugiTransactionException {
        LOG.trace("statementResult close start");

        Throwable occurred = null;
        try {
            if (enableCheckResultOnClose()) {
                if (this.checkResultOnClose) {
                    checkLowResult();
                }
            }
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            try {
                // not try-finally
                IceaxeIoUtil.closeInTransaction(timeoutNanos, IceaxeErrorCode.RESULT_CLOSE_TIMEOUT, IceaxeErrorCode.RESULT_CLOSE_ERROR, //
                        lowResultFuture);
                super.close();
            } catch (TsurugiTransactionException e) {
                fillToTsurugiException(e);
                if (occurred != null) {
                    occurred.addSuppressed(e);
                } else {
                    occurred = e;
                    throw e;
                }
            } catch (Throwable e) {
                if (occurred != null) {
                    occurred.addSuppressed(e);
                } else {
                    occurred = e;
                    throw e;
                }
            } finally {
                var finalOccurred = occurred;
                event(occurred, listener -> listener.closeResult(this, timeoutNanos, finalOccurred));
            }
        }

        LOG.trace("statementResult close end");
    }
}
