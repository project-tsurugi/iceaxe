/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.iceaxe.transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.sql.result.TgResultCount;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.transaction.event.TsurugiTransactionEventListener;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.transaction.status.TgTxStatus;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.IceaxeTimeoutCloseable;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.iceaxe.util.function.IoFunction;
import com.tsurugidb.iceaxe.util.function.TsurugiTransactionConsumer;
import com.tsurugidb.iceaxe.util.function.TsurugiTransactionConsumerWithRowNumber;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi Transaction.
 */
public class TsurugiTransaction implements IceaxeTimeoutCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiTransaction.class);

    private static final AtomicInteger TRANSACTION_COUNT = new AtomicInteger(0);
    private static final AtomicInteger TX_EXECUTE_COUNT = new AtomicInteger(0);

    private final int iceaxeTxId;
    private final TsurugiSession ownerSession;
    private final TgTxOption txOption;
    private FutureResponse<Transaction> lowTransactionFuture;
    private Throwable lowFutureException = null;
    private Transaction lowTransaction;
    private boolean calledGetLowTransaction = false;
    private String transactionId = null;
    private TsurugiTransactionManager ownerTm = null;
    private int iceaxeTmExecuteId = 0;
    private int attempt = 0;
    private boolean rollbackOnClose;
    private final IceaxeTimeout beginTimeout;
    private final IceaxeTimeout commitTimeout;
    private final IceaxeTimeout rollbackTimeout;
    private final IceaxeTimeout closeTimeout;
    private List<TsurugiTransactionEventListener> eventListenerList = null;
    private boolean finishCalled = false;
    private boolean committed = false;
    private boolean rollbacked = false;
    private final IceaxeCloseableSet closeableSet = new IceaxeCloseableSet();
    private volatile boolean closed = false;

    /**
     * Creates a new instance.
     * <p>
     * Call {@link #initialize(FutureResponse)} after construct.
     * </p>
     *
     * @param session  session
     * @param txOption transaction option
     */
    @IceaxeInternal
    public TsurugiTransaction(TsurugiSession session, TgTxOption txOption) {
        this.iceaxeTxId = TRANSACTION_COUNT.incrementAndGet();
        this.ownerSession = session;
        this.txOption = txOption;
        this.rollbackOnClose = txOption.isRollbackOnTransactionClose();

        var sessionOption = session.getSessionOption();
        this.beginTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.TRANSACTION_BEGIN);
        this.commitTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.TRANSACTION_COMMIT);
        this.rollbackTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.TRANSACTION_ROLLBACK);
        this.closeTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.TRANSACTION_CLOSE);
    }

    /**
     * initialize.
     * <p>
     * Call this method only once after construct.
     * </p>
     *
     * @param lowTransactionFuture future of Transaction
     * @throws IOException if session already closed
     */
    @IceaxeInternal
    public void initialize(FutureResponse<Transaction> lowTransactionFuture) throws IOException {
        if (this.lowTransactionFuture != null || this.lowTransaction != null) {
            throw new IllegalStateException("initialize() is already called");
        }

        this.lowTransactionFuture = Objects.requireNonNull(lowTransactionFuture);

        try {
            ownerSession.addChild(this);
        } catch (Throwable e) {
            LOG.trace("transaction.initialize close start", e);
            try {
                IceaxeIoUtil.close(closeTimeout.getNanos(), IceaxeErrorCode.TX_CLOSE_TIMEOUT, IceaxeErrorCode.TX_CLOSE_ERROR, //
                        lowTransactionFuture);
            } catch (Throwable c) {
                e.addSuppressed(c);
            }
            LOG.trace("transaction.initialize close end");
            throw e;
        }
    }

    /**
     * get iceaxe transactionId.
     *
     * @return iceaxe transactionId
     */
    public int getIceaxeTxId() {
        return this.iceaxeTxId;
    }

    /**
     * get session.
     *
     * @return session
     */
    public @Nonnull TsurugiSession getSession() {
        return this.ownerSession;
    }

    /**
     * get transaction option.
     *
     * @return transaction option
     */
    public @Nonnull TgTxOption getTransactionOption() {
        return this.txOption;
    }

    /**
     * set owner information.
     *
     * @param tm                owner transaction manager
     * @param iceaxeTmExecuteId iceaxe tm executeId
     * @param attempt           attempt number
     */
    public void setOwner(TsurugiTransactionManager tm, int iceaxeTmExecuteId, int attempt) {
        this.ownerTm = tm;
        this.iceaxeTmExecuteId = iceaxeTmExecuteId;
        this.attempt = attempt;
    }

    /**
     * get transaction manager.
     *
     * @return transaction manager, null if this transaction is not created by transaction manager
     */
    public @Nullable TsurugiTransactionManager getTransactionManager() {
        return this.ownerTm;
    }

    /**
     * get iceaxe tm executeId.
     *
     * @return iceaxe tm executeId, 0 if this transaction is not created by transaction manager
     */
    public int getIceaxeTmExecuteId() {
        return this.iceaxeTmExecuteId;
    }

    /**
     * get attempt number.
     *
     * @return attempt number, 0 if this transaction is not created by transaction manager
     */
    public int getAttempt() {
        return this.attempt;
    }

    /**
     * set rollback on close.
     *
     * @param rollback {@code true} if rollback on close
     * @since 1.9.0
     */
    public void setRollbackOnClose(boolean rollback) {
        this.rollbackOnClose = rollback;
    }

    /**
     * get rollback on close.
     *
     * @return {@code true} if rollback on close
     * @since 1.9.0
     */
    public boolean isRollbackOnClose() {
        return this.rollbackOnClose;
    }

    /**
     * set transaction-begin-timeout.
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setBeginTimeout(long time, TimeUnit unit) {
        setBeginTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set transaction-begin-timeout.
     *
     * @param timeout time
     */
    public void setBeginTimeout(TgTimeValue timeout) {
        beginTimeout.set(timeout);
    }

    /**
     * set transaction-commit-timeout.
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setCommitTimeout(long time, TimeUnit unit) {
        setCommitTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set transaction-commit-timeout.
     *
     * @param timeout time
     */
    public void setCommitTimeout(TgTimeValue timeout) {
        commitTimeout.set(timeout);
    }

    /**
     * set transaction-rollback-timeout.
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setRollbackTimeout(long time, TimeUnit unit) {
        setRollbackTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set transaction-rollback-timeout.
     *
     * @param timeout time
     */
    public void setRollbackTimeout(TgTimeValue timeout) {
        rollbackTimeout.set(timeout);
    }

    /**
     * set transaction-close-timeout.
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setCloseTimeout(long time, TimeUnit unit) {
        setCloseTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set transaction-close-timeout.
     *
     * @param timeout time
     */
    public void setCloseTimeout(TgTimeValue timeout) {
        closeTimeout.set(timeout);
    }

    /**
     * add event listener.
     *
     * @param listener event listener
     * @return this
     */
    public TsurugiTransaction addEventListener(TsurugiTransactionEventListener listener) {
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
    public Optional<TsurugiTransactionEventListener> findEventListener(Predicate<TsurugiTransactionEventListener> predicate) {
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

    private void event(Throwable occurred, Consumer<TsurugiTransactionEventListener> action) {
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
     * get session option.
     *
     * @return session option
     */
    public final TgSessionOption getSessionOption() {
        return ownerSession.getSessionOption();
    }

    /**
     * convert to {@link Transaction}.
     *
     * @return transaction
     * @throws IOException          if an I/O error occurs while retrieving transaction
     * @throws InterruptedException if interrupted while retrieving transaction
     */
    @IceaxeInternal
//  @ThreadSafe
    public final synchronized Transaction getLowTransaction() throws IOException, InterruptedException {
        return getLowTransaction(beginTimeout);
    }

    private Transaction getLowTransaction(IceaxeTimeout beginTimeout) throws IOException, InterruptedException {
        this.calledGetLowTransaction = true;
        if (this.lowTransaction == null) {
            if (this.lowFutureException != null) {
                throw new IceaxeIOException(IceaxeErrorCode.TX_LOW_ERROR, lowFutureException);
            }
            if (this.lowTransactionFuture == null) {
                throw new IllegalStateException("initialize() is not called");
            }

            LOG.trace("lowTransaction get start");
            event(null, listener -> listener.lowTransactionGetStart(this));
            try {
                this.lowTransaction = IceaxeIoUtil.getAndCloseFuture(lowTransactionFuture, //
                        beginTimeout, IceaxeErrorCode.TX_BEGIN_TIMEOUT, //
                        IceaxeErrorCode.TX_CLOSE_TIMEOUT);
            } catch (Throwable e) {
                this.lowFutureException = e;
                event(e, listener -> listener.lowTransactionGetEnd(this, null, e));
                throw e;
            }
            LOG.trace("lowTransaction get end");

            this.lowTransactionFuture = null;

            this.transactionId = lowTransaction.getTransactionId();
            event(null, listener -> listener.lowTransactionGetEnd(this, transactionId, null));
        }
        return this.lowTransaction;
    }

    /**
     * Provides transaction id that is unique to for the duration of the database server's lifetime.
     *
     * @return the id String for this transaction
     * @throws IOException          if an I/O error occurs while retrieving transaction
     * @throws InterruptedException if interrupted while retrieving transaction
     */
    public String getTransactionId() throws IOException, InterruptedException {
        if (this.transactionId == null) {
            var lowTx = getLowTransaction();
            if (this.transactionId == null) {
                this.transactionId = lowTx.getTransactionId();
            }
        }
        return this.transactionId;
    }

    // execute statement

    /**
     * Tsurugi transaction execute method.
     */
    public enum TgTxMethod {
        /** execute DDL */
        EXECUTE_DDL("executeDdl"),
        /** execute query */
        EXECUTE_QUERY("executeQuery"),
        /** execute statement */
        EXECUTE_STATEMENT("executeStatement"),
        /** execute batch */
        EXECUTE_BATCH("executeBatch"),
        /** execute and for each */
        EXECUTE_FOR_EACH("executeAndForEach"),
        /** execute and get list */
        EXECUTE_GET_LIST("executeAndGetList"),
        /** execute and find record */
        EXECUTE_FIND_RECORD("executeAndFindRecord"),
        /** execute and get count */
        EXECUTE_GET_COUNT("executeAndGetCount"),
        /** execute and get count detail */
        EXECUTE_GET_COUNT_DETAIL("executeAndGetCountDetail"),
        /** commit */
        COMMIT("commit"),
        /** rollback */
        ROLLBACK("rollback"),
        /** get transaction status */
        GET_TRANSACTION_STATUS("getTransactionStatus");

        private final String methodName;

        private TgTxMethod(String methodName) {
            this.methodName = methodName;
        }

        /**
         * get method name.
         *
         * @return method name
         */
        public String getMethodName() {
            return this.methodName;
        }
    }

    private int getNewIceaxeTxExecuteId() {
        return TX_EXECUTE_COUNT.incrementAndGet();
    }

    /**
     * execute DDL.
     *
     * @param sql DDL
     * @throws IOException                 if an I/O error occurs while execute DDL
     * @throws InterruptedException        if interrupted while execute DDL
     * @throws TsurugiTransactionException if server error occurs while execute DDL
     */
    public void executeDdl(String sql) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_DDL;
        int txExecuteId = getNewIceaxeTxExecuteId();
        try (var ps = ownerSession.createStatement(sql)) {
            event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, null));

            TsurugiStatementResult result = null;
            Throwable occurred = null;
            try (var rs = ps.execute(this)) {
                result = rs;
                rs.checkLowResult();
            } catch (TsurugiTransactionException e) {
                occurred = e;
                e.setTxMethod(method, txExecuteId);
                throw e;
            } catch (Throwable e) {
                occurred = e;
                throw e;
            } finally {
                var finalResult = result;
                var finalOccurred = occurred;
                event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, null, finalResult, finalOccurred));
            }
        }
    }

    /**
     * execute query.
     *
     * @param <R> result type
     * @param ps  SQL definition
     * @return SQL result ({@link java.io.Closeable})
     * @throws IOException                 if an I/O error occurs while execute query
     * @throws InterruptedException        if interrupted while execute query
     * @throws TsurugiTransactionException if server error occurs while execute query
     * @see #executeAndForEach(TsurugiSqlQuery, TsurugiTransactionConsumer)
     * @see #executeAndFindRecord(TsurugiSqlQuery)
     * @see #executeAndGetList(TsurugiSqlQuery)
     */
    public <R> TsurugiQueryResult<R> executeQuery(TsurugiSqlQuery<R> ps) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_QUERY;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, null));

        TsurugiQueryResult<R> result = null;
        Throwable occurred = null;
        try {
            result = ps.execute(this);
            return result;
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, null, finalResult, finalOccurred));
        }
    }

    /**
     * execute query.
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param ps        SQL definition
     * @param parameter SQL parameter
     * @return SQL result ({@link java.io.Closeable})
     * @throws IOException                 if an I/O error occurs while execute query
     * @throws InterruptedException        if interrupted while execute query
     * @throws TsurugiTransactionException if server error occurs while execute query
     * @see #executeAndForEach(TsurugiSqlPreparedQuery, Object, TsurugiTransactionConsumer)
     * @see #executeAndFindRecord(TsurugiSqlPreparedQuery, Object)
     * @see #executeAndGetList(TsurugiSqlPreparedQuery, Object)
     */
    public <P, R> TsurugiQueryResult<R> executeQuery(TsurugiSqlPreparedQuery<P, R> ps, P parameter) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_QUERY;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, parameter));

        TsurugiQueryResult<R> result = null;
        Throwable occurred = null;
        try {
            result = ps.execute(this, parameter);
            return result;
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, parameter, finalResult, finalOccurred));
        }
    }

    /**
     * execute statement.
     *
     * @param ps SQL definition
     * @return SQL result ({@link java.io.Closeable})
     * @throws IOException                 if an I/O error occurs while execute statement
     * @throws InterruptedException        if interrupted while execute statement
     * @throws TsurugiTransactionException if server error occurs while execute statement
     * @see #executeAndGetCount(TsurugiSqlStatement)
     */
    public TsurugiStatementResult executeStatement(TsurugiSqlStatement ps) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_STATEMENT;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, null));

        TsurugiStatementResult result = null;
        Throwable occurred = null;
        try {
            result = ps.execute(this);
            return result;
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, null, finalResult, finalOccurred));
        }
    }

    /**
     * execute statement.
     *
     * @param <P>       parameter type
     * @param ps        SQL definition
     * @param parameter SQL parameter
     * @return SQL result ({@link java.io.Closeable})
     * @throws IOException                 if an I/O error occurs while execute statement
     * @throws InterruptedException        if interrupted while execute statement
     * @throws TsurugiTransactionException if server error occurs while execute statement
     * @see #executeAndGetCount(TsurugiSqlPreparedStatement, Object)
     */
    public <P> TsurugiStatementResult executeStatement(TsurugiSqlPreparedStatement<P> ps, P parameter) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_STATEMENT;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, parameter));

        TsurugiStatementResult result = null;
        Throwable occurred = null;
        try {
            result = ps.execute(this, parameter);
            return result;
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, parameter, finalResult, finalOccurred));
        }
    }

    /**
     * execute batch.
     *
     * @param <P>           parameter type
     * @param ps            SQL definition
     * @param parameterList SQL parameter
     * @return SQL result ({@link java.io.Closeable})
     * @throws IOException                 if an I/O error occurs while execute batch
     * @throws InterruptedException        if interrupted while execute batch
     * @throws TsurugiTransactionException if server error occurs while execute batch
     * @see #executeAndGetCount(TsurugiSqlPreparedStatement, Collection)
     */
    public <P> TsurugiStatementResult executeBatch(TsurugiSqlPreparedStatement<P> ps, Collection<P> parameterList) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_BATCH;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, parameterList));

        TsurugiStatementResult result = null;
        Throwable occurred = null;
        try {
            result = ps.executeBatch(this, parameterList);
            return result;
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, parameterList, finalResult, finalOccurred));
        }
    }

    /**
     * execute query.
     *
     * @param <R>    result type
     * @param ps     SQL definition
     * @param action The action to be performed for each record
     * @throws IOException                 if an I/O error occurs while execute query
     * @throws InterruptedException        if interrupted while execute query
     * @throws TsurugiTransactionException if server error occurs while execute query
     */
    public <R> void executeAndForEach(TsurugiSqlQuery<R> ps, TsurugiTransactionConsumer<R> action) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_FOR_EACH;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, null));

        TsurugiQueryResult<R> result = null;
        Throwable occurred = null;
        try (var rs = ps.execute(this)) {
            result = rs;
            rs.whileEach(action);
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, null, finalResult, finalOccurred));
        }
    }

    /**
     * execute query.
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param ps        SQL definition
     * @param parameter SQL parameter
     * @param action    The action to be performed for each record
     * @throws IOException                 if an I/O error occurs while execute query
     * @throws InterruptedException        if interrupted while execute query
     * @throws TsurugiTransactionException if server error occurs while execute query
     */
    public <P, R> void executeAndForEach(TsurugiSqlPreparedQuery<P, R> ps, P parameter, TsurugiTransactionConsumer<R> action) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_FOR_EACH;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, parameter));

        TsurugiQueryResult<R> result = null;
        Throwable occurred = null;
        try (var rs = ps.execute(this, parameter)) {
            result = rs;
            rs.whileEach(action);
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, parameter, finalResult, finalOccurred));
        }
    }

    /**
     * execute query.
     *
     * @param <R>    result type
     * @param ps     SQL definition
     * @param action The action to be performed for each record
     * @throws IOException                 if an I/O error occurs while execute query
     * @throws InterruptedException        if interrupted while execute query
     * @throws TsurugiTransactionException if server error occurs while execute query
     * @since 1.9.0
     */
    public <R> void executeAndForEach(TsurugiSqlQuery<R> ps, TsurugiTransactionConsumerWithRowNumber<R> action) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_FOR_EACH;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, null));

        TsurugiQueryResult<R> result = null;
        Throwable occurred = null;
        try (var rs = ps.execute(this)) {
            result = rs;
            rs.whileEach(action);
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, null, finalResult, finalOccurred));
        }
    }

    /**
     * execute query.
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param ps        SQL definition
     * @param parameter SQL parameter
     * @param action    The action to be performed for each record
     * @throws IOException                 if an I/O error occurs while execute query
     * @throws InterruptedException        if interrupted while execute query
     * @throws TsurugiTransactionException if server error occurs while execute query
     * @since 1.9.0
     */
    public <P, R> void executeAndForEach(TsurugiSqlPreparedQuery<P, R> ps, P parameter, TsurugiTransactionConsumerWithRowNumber<R> action)
            throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_FOR_EACH;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, parameter));

        TsurugiQueryResult<R> result = null;
        Throwable occurred = null;
        try (var rs = ps.execute(this, parameter)) {
            result = rs;
            rs.whileEach(action);
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, parameter, finalResult, finalOccurred));
        }
    }

    /**
     * execute query.
     *
     * @param <R> result type
     * @param ps  SQL definition
     * @return list of record
     * @throws IOException                 if an I/O error occurs while execute query
     * @throws InterruptedException        if interrupted while execute query
     * @throws TsurugiTransactionException if server error occurs while execute query
     */
    public <R> List<R> executeAndGetList(TsurugiSqlQuery<R> ps) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_GET_LIST;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, null));

        TsurugiQueryResult<R> result = null;
        Throwable occurred = null;
        try (var rs = ps.execute(this)) {
            result = rs;
            return rs.getRecordList();
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, null, finalResult, finalOccurred));
        }
    }

    /**
     * execute query.
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param ps        SQL definition
     * @param parameter SQL parameter
     * @return list of record
     * @throws IOException                 if an I/O error occurs while execute query
     * @throws InterruptedException        if interrupted while execute query
     * @throws TsurugiTransactionException if server error occurs while execute query
     */
    public <P, R> List<R> executeAndGetList(TsurugiSqlPreparedQuery<P, R> ps, P parameter) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_GET_LIST;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, parameter));

        TsurugiQueryResult<R> result = null;
        Throwable occurred = null;
        try (var rs = ps.execute(this, parameter)) {
            result = rs;
            return rs.getRecordList();
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, parameter, finalResult, finalOccurred));
        }
    }

    /**
     * execute query.
     *
     * @param <R> result type
     * @param ps  SQL definition
     * @return record
     * @throws IOException                 if an I/O error occurs while execute query
     * @throws InterruptedException        if interrupted while execute query
     * @throws TsurugiTransactionException if server error occurs while execute query
     */
    public <R> Optional<R> executeAndFindRecord(TsurugiSqlQuery<R> ps) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_FIND_RECORD;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, null));

        TsurugiQueryResult<R> result = null;
        Throwable occurred = null;
        try (var rs = ps.execute(this)) {
            result = rs;
            return rs.findRecord();
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, null, finalResult, finalOccurred));
        }
    }

    /**
     * execute query.
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param ps        SQL definition
     * @param parameter SQL parameter
     * @return record
     * @throws IOException                 if an I/O error occurs while execute query
     * @throws InterruptedException        if interrupted while execute query
     * @throws TsurugiTransactionException if server error occurs while execute query
     */
    public <P, R> Optional<R> executeAndFindRecord(TsurugiSqlPreparedQuery<P, R> ps, P parameter) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_FIND_RECORD;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, parameter));

        TsurugiQueryResult<R> result = null;
        Throwable occurred = null;
        try (var rs = ps.execute(this, parameter)) {
            result = rs;
            return rs.findRecord();
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, parameter, finalResult, finalOccurred));
        }
    }

    /**
     * execute statement.
     *
     * @param ps SQL definition
     * @return row count
     * @throws IOException                 if an I/O error occurs while execute statement
     * @throws InterruptedException        if interrupted while execute statement
     * @throws TsurugiTransactionException if server error occurs while execute statement
     */
    public int executeAndGetCount(TsurugiSqlStatement ps) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_GET_COUNT;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, null));

        TsurugiStatementResult result = null;
        Throwable occurred = null;
        try (var rs = ps.execute(this)) {
            result = rs;
            return rs.getUpdateCount();
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, null, finalResult, finalOccurred));
        }
    }

    /**
     * execute statement.
     *
     * @param <P>       parameter type
     * @param ps        SQL definition
     * @param parameter SQL parameter
     * @return row count
     * @throws IOException                 if an I/O error occurs while execute statement
     * @throws InterruptedException        if interrupted while execute statement
     * @throws TsurugiTransactionException if server error occurs while execute statement
     */
    public <P> int executeAndGetCount(TsurugiSqlPreparedStatement<P> ps, P parameter) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_GET_COUNT;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, parameter));

        TsurugiStatementResult result = null;
        Throwable occurred = null;
        try (var rs = ps.execute(this, parameter)) {
            result = rs;
            return rs.getUpdateCount();
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, parameter, finalResult, finalOccurred));
        }
    }

    /**
     * execute batch.
     *
     * @param <P>           parameter type
     * @param ps            SQL definition
     * @param parameterList SQL parameter
     * @return row count
     * @throws IOException                 if an I/O error occurs while execute batch
     * @throws InterruptedException        if interrupted while execute batch
     * @throws TsurugiTransactionException if server error occurs while execute batch
     */
    public <P> int executeAndGetCount(TsurugiSqlPreparedStatement<P> ps, Collection<P> parameterList) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_GET_COUNT;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, parameterList));

        TsurugiStatementResult result = null;
        Throwable occurred = null;
        try (var rs = ps.executeBatch(this, parameterList)) {
            result = rs;
            return rs.getUpdateCount();
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, parameterList, finalResult, finalOccurred));
        }
    }

    /**
     * execute statement.
     *
     * @param ps SQL definition
     * @return row count
     * @throws IOException                 if an I/O error occurs while execute statement
     * @throws InterruptedException        if interrupted while execute statement
     * @throws TsurugiTransactionException if server error occurs while execute statement
     * @since 1.1.0
     */
    public TgResultCount executeAndGetCountDetail(TsurugiSqlStatement ps) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_GET_COUNT_DETAIL;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, null));

        TsurugiStatementResult result = null;
        Throwable occurred = null;
        try (var rs = ps.execute(this)) {
            result = rs;
            return rs.getCountDetail();
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, null, finalResult, finalOccurred));
        }
    }

    /**
     * execute statement.
     *
     * @param <P>       parameter type
     * @param ps        SQL definition
     * @param parameter SQL parameter
     * @return row count
     * @throws IOException                 if an I/O error occurs while execute statement
     * @throws InterruptedException        if interrupted while execute statement
     * @throws TsurugiTransactionException if server error occurs while execute statement
     * @since 1.1.0
     */
    public <P> TgResultCount executeAndGetCountDetail(TsurugiSqlPreparedStatement<P> ps, P parameter) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_GET_COUNT_DETAIL;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, parameter));

        TsurugiStatementResult result = null;
        Throwable occurred = null;
        try (var rs = ps.execute(this, parameter)) {
            result = rs;
            return rs.getCountDetail();
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, parameter, finalResult, finalOccurred));
        }
    }

    /**
     * execute batch.
     *
     * @param <P>           parameter type
     * @param ps            SQL definition
     * @param parameterList SQL parameter
     * @return row count
     * @throws IOException                 if an I/O error occurs while execute batch
     * @throws InterruptedException        if interrupted while execute batch
     * @throws TsurugiTransactionException if server error occurs while execute batch
     * @since 1.1.0
     */
    public <P> TgResultCount executeAndGetCountDetail(TsurugiSqlPreparedStatement<P> ps, Collection<P> parameterList) throws IOException, InterruptedException, TsurugiTransactionException {
        var method = TgTxMethod.EXECUTE_GET_COUNT_DETAIL;
        int txExecuteId = getNewIceaxeTxExecuteId();
        event(null, listener -> listener.executeStart(this, method, txExecuteId, ps, parameterList));

        TsurugiStatementResult result = null;
        Throwable occurred = null;
        try (var rs = ps.executeBatch(this, parameterList)) {
            result = rs;
            return rs.getCountDetail();
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(method, txExecuteId);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalResult = result;
            var finalOccurred = occurred;
            event(occurred, listener -> listener.executeEnd(this, method, txExecuteId, ps, parameterList, finalResult, finalOccurred));
        }
    }

    /**
     * transaction task.
     *
     * @param <R> type of return value
     */
    @IceaxeInternal
    @FunctionalInterface
    public interface LowTransactionTask<R> {
        /**
         * execute transaction task.
         *
         * @param lowTransaction transaction
         * @return return value
         * @throws IOException          if an I/O error occurs while execute
         * @throws InterruptedException if interrupted while execute
         */
        R run(Transaction lowTransaction) throws IOException, InterruptedException;
    }

    /**
     * execute transaction task.
     *
     * @param <R>  type of return value
     * @param task transaction task
     * @return return value
     * @throws IOException          if an I/O error occurs while execute
     * @throws InterruptedException if interrupted while execute
     */
    @IceaxeInternal
    public <R> R executeLow(LowTransactionTask<R> task) throws IOException, InterruptedException {
        checkClose();
        return task.run(getLowTransaction());
    }

    // commit, rollback

    /**
     * Whether transaction is available.
     *
     * @return {@code true} if available
     * @throws IOException          if an I/O error occurs while retrieving transaction
     * @throws InterruptedException if interrupted while retrieving transaction
     */
//  @ThreadSafe
    public final synchronized boolean available() throws IOException, InterruptedException {
        if (isClosed()) {
            return false;
        }
        if (!this.calledGetLowTransaction) {
            getLowTransaction();
        }
        return this.lowTransaction != null;
    }

    /**
     * get transaction status.
     *
     * @return transaction status
     * @throws IOException          if an I/O error occurs while retrieving transaction status
     * @throws InterruptedException if interrupted while retrieving transaction status
     */
//  @ThreadSafe
    public TgTxStatus getTransactionStatus() throws IOException, InterruptedException {
        checkClose();
        var helper = getSession().getTransactionStatusHelper();
        return helper.getTransactionStatus(this);
    }

    /**
     * do commit.
     *
     * @param commitType commit type
     * @throws IOException                 if an I/O error occurs while commit
     * @throws InterruptedException        if interrupted while commit
     * @throws TsurugiTransactionException if server error occurs while commit
     * @see #commit(TgCommitOption)
     */
    public void commit(TgCommitType commitType) throws IOException, InterruptedException, TsurugiTransactionException {
        var commitOption = TgCommitOption.of(commitType);
        commit(commitOption);
    }

    /**
     * do commit.
     *
     * @param commitOption commit option
     * @throws IOException                 if an I/O error occurs while commit
     * @throws InterruptedException        if interrupted while commit
     * @throws TsurugiTransactionException if server error occurs while commit
     * @since 1.10.0
     */
    public synchronized void commit(TgCommitOption commitOption) throws IOException, InterruptedException, TsurugiTransactionException {
        this.finishCalled = true;
        checkClose();
        if (this.committed) {
            return;
        }
        if (this.rollbacked) {
            throw new IllegalStateException("rollback has already been called");
        }

        LOG.trace("transaction commit start. commitOption={}", commitOption);
        event(null, listener -> listener.commitStart(this, commitOption));

        Throwable occurred = null;
        try {
            long start = System.nanoTime();
            closeableSet.closeInTransaction(commitTimeout.getNanos(), IceaxeErrorCode.TX_COMMIT_CHILD_CLOSE_ERROR);
            var lowCommitOption = commitOption.toLowCommitOption();
            finish(start, lowTx -> lowTx.commit(lowCommitOption), commitTimeout, IceaxeErrorCode.TX_COMMIT_TIMEOUT, IceaxeErrorCode.TX_COMMIT_CLOSE_TIMEOUT);
            this.committed = true;
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(TgTxMethod.COMMIT, 0);
            throw e;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalOccurred = occurred;
            event(occurred, listener -> listener.commitEnd(this, commitOption, finalOccurred));
        }

        LOG.trace("transaction commit end");
    }

    /**
     * do rollback.
     *
     * @throws IOException                 if an I/O error occurs while rollback
     * @throws InterruptedException        if interrupted while rollback
     * @throws TsurugiTransactionException if server error occurs while rollback
     */
    public synchronized void rollback() throws IOException, InterruptedException, TsurugiTransactionException {
        this.finishCalled = true;
        checkClose();
        if (this.committed || this.rollbacked) {
            return;
        }

        rollback(rollbackTimeout);
    }

    private void rollback(IceaxeTimeout rollbackTimeout) throws IOException, InterruptedException, TsurugiTransactionException {
        LOG.trace("transaction rollback start");
        event(null, listener -> listener.rollbackStart(this));

        Throwable occurred = null;
        List<Throwable> saveList = List.of();
        try {
            long start = System.nanoTime();
            saveList = closeableSet.close(rollbackTimeout.getNanos());
            finish(start, Transaction::rollback, rollbackTimeout, IceaxeErrorCode.TX_ROLLBACK_TIMEOUT, IceaxeErrorCode.TX_ROLLBACK_CLOSE_TIMEOUT);
            this.rollbacked = true;

            if (!saveList.isEmpty()) {
                Throwable e = null;
                for (var save : saveList) {
                    if (e == null) {
                        e = closeableSet.convertExceptionInTransaction(save, IceaxeErrorCode.TX_ROLLBACK_CHILD_CLOSE_ERROR);
                    } else {
                        e.addSuppressed(save);
                    }
                }
                saveList = List.of();
                closeableSet.throwExceptionInTransaction(e); // throw exception
                throw new InternalError(e); // don't come here
            }
        } catch (TsurugiTransactionException e) {
            occurred = e;
            e.setTxMethod(TgTxMethod.ROLLBACK, 0);
            for (var save : saveList) {
                e.addSuppressed(save);
            }
            throw e;
        } catch (Throwable e) {
            occurred = e;
            for (var save : saveList) {
                e.addSuppressed(save);
            }
            throw e;
        } finally {
            var finalOccurred = occurred;
            event(occurred, listener -> listener.rollbackEnd(this, finalOccurred));
        }

        LOG.trace("transaction rollback end");
    }

    /**
     * commit/rollback.
     *
     * @param start                 start time
     * @param finisher              commit/rollback function
     * @param timeout               timeout
     * @param timeoutErrorCode      error code for timeout
     * @param closeTimeoutErrorCode error code for close timeout
     * @throws IOException                 if an I/O error occurs while execute
     * @throws InterruptedException        if interrupted while execute
     * @throws TsurugiTransactionException if server error occurs while execute
     */
    protected void finish(long start, IoFunction<Transaction, FutureResponse<Void>> finisher, IceaxeTimeout timeout, IceaxeErrorCode timeoutErrorCode, IceaxeErrorCode closeTimeoutErrorCode)
            throws IOException, InterruptedException, TsurugiTransactionException {
        var transaction = getLowTransaction(timeout);
        var lowResultFuture = finisher.apply(transaction);
        long timeoutNanos = IceaxeIoUtil.calculateTimeoutNanos(timeout.getNanos(), start);
        var finishTimeout = new IceaxeTimeout(timeoutNanos, TimeUnit.NANOSECONDS);
        IceaxeIoUtil.getAndCloseFutureInTransaction(lowResultFuture, finishTimeout, timeoutErrorCode, closeTimeoutErrorCode);
    }

    /**
     * get committed.
     *
     * @return {@code true} if committed
     */
    public synchronized boolean isCommitted() {
        return this.committed;
    }

    /**
     * get rollbacked.
     *
     * @return {@code true} if rollbacked
     */
    public synchronized boolean isRollbacked() {
        return this.rollbacked;
    }

    // child

    /**
     * add child object.
     *
     * @param closeable child object
     * @throws IOException if already closed
     */
    @IceaxeInternal
    public void addChild(IceaxeTimeoutCloseable closeable) throws IOException {
        checkClose();
        closeableSet.add(closeable);
    }

    /**
     * remove child object.
     *
     * @param closeable child object
     */
    @IceaxeInternal
    public void removeChild(IceaxeTimeoutCloseable closeable) {
        closeableSet.remove(closeable);
    }

    // close

    @Override
    public void close() throws IOException, InterruptedException {
        close(closeTimeout.getNanos());
    }

    @Override
    public void close(long timeoutNanos) throws IOException, InterruptedException {
        this.closed = true;

        if (LOG.isTraceEnabled()) {
            LOG.trace("transaction close start. committed={}, rollbacked={}", committed, rollbacked);
        }
        Throwable occurred = null;
        try {
            var rollback = rollbackCloser();

            IceaxeIoUtil.close(timeoutNanos, closeableSet, IceaxeErrorCode.TX_CHILD_CLOSE_ERROR, t -> {
                // not try-finally
                IceaxeIoUtil.close(t, IceaxeErrorCode.TX_CLOSE_TIMEOUT, IceaxeErrorCode.TX_CLOSE_ERROR, //
                        rollback, lowTransaction, lowTransactionFuture);
                ownerSession.removeChild(this);
            });
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var finalOccurred = occurred;
            event(occurred, listener -> listener.closeTransaction(this, timeoutNanos, finalOccurred));
        }
        LOG.trace("transaction close end");
    }

    private IceaxeTimeoutCloseable rollbackCloser() {
        if (!this.rollbackOnClose) {
            return null;
        }

        if (this.finishCalled) {
            // commit()またはrollback()が既に呼ばれているので、ロールバック不要
            return null;
        }
        if (this.lowTransaction == null) {
            // lowTransactionを取得していない、すなわち何のトランザクション処理も行っていないので、ロールバックは（FutureResponseのクローズで）Tsubakuroに任せる
            return null;
        }

        return timeoutNanos -> {
            this.finishCalled = true;
            var timeout = new IceaxeTimeout(timeoutNanos, TimeUnit.NANOSECONDS);
            rollback(timeout);
        };
    }

    /**
     * Returns the closed state of the transaction.
     *
     * @return {@code true} if the transaction has been closed
     * @see #close()
     */
    public boolean isClosed() {
        return this.closed;
    }

    /**
     * check close.
     *
     * @throws IOException if already closed
     */
    protected void checkClose() throws IOException {
        if (isClosed()) {
            throw new IceaxeIOException(IceaxeErrorCode.TX_ALREADY_CLOSED);
        }
    }

    @Override
    public String toString() {
        return "TsurugiTransaction(" + txOption + ", iceaxeTxId=" + iceaxeTxId + ", iceaxeTmExecuteId=" + iceaxeTmExecuteId + ", attempt=" + attempt + ", transactionId=" + transactionId + ")";
    }
}
