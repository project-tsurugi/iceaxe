package com.tsurugidb.iceaxe.transaction;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.result.TsurugiResultCount;
import com.tsurugidb.iceaxe.result.TsurugiResultSet;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery1;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.iceaxe.util.function.IoFunction;
import com.tsurugidb.iceaxe.util.function.TsurugiTransactionConsumer;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi Transaction
 */
public class TsurugiTransaction implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiTransaction.class);

    private final TsurugiSession ownerSession;
    private FutureResponse<Transaction> lowTransactionFuture;
    private Throwable lowFutureException = null;
    private Transaction lowTransaction;
    private boolean calledGetLowTransaction = false;
    private final TgTxOption txOption;
    private TsurugiTransactionManager txManager = null;
    private int attempt = 0;
    private final IceaxeTimeout beginTimeout;
    private final IceaxeTimeout commitTimeout;
    private final IceaxeTimeout rollbackTimeout;
    private final IceaxeTimeout closeTimeout;
    private List<Consumer<TsurugiTransaction>> beforeCommitListenerList;
    private List<Consumer<TsurugiTransaction>> commitListenerList;
    private List<Consumer<TsurugiTransaction>> rollbackListenerList;
    private boolean committed = false;
    private boolean rollbacked = false;
    private final IceaxeCloseableSet closeableSet = new IceaxeCloseableSet();
    private boolean closed = false;

    // internal
    public TsurugiTransaction(TsurugiSession session, FutureResponse<Transaction> lowTransactionFuture, TgTxOption option) throws IOException {
        this.ownerSession = session;
        this.lowTransactionFuture = lowTransactionFuture;
        this.txOption = option;
        session.addChild(this);

        var info = session.getSessionInfo();
        this.beginTimeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_BEGIN);
        this.commitTimeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_COMMIT);
        this.rollbackTimeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_ROLLBACK);
        this.closeTimeout = new IceaxeTimeout(info, TgTimeoutKey.TRANSACTION_CLOSE);

        applyCloseTimeout();
    }

    private void applyCloseTimeout() {
        closeTimeout.apply(lowTransaction);
        closeTimeout.apply(lowTransactionFuture);
    }

    /**
     * get session
     *
     * @return session
     */
    @Nonnull
    public TsurugiSession getSession() {
        return this.ownerSession;
    }

    /**
     * get transaction option.
     *
     * @return transaction option
     */
    @Nonnull
    public TgTxOption getTransactionOption() {
        return this.txOption;
    }

    /**
     * set owner information.
     *
     * @param tm      owner transaction manager
     * @param attempt attempt number
     */
    public void setOwner(TsurugiTransactionManager tm, int attempt) {
        this.txManager = tm;
        this.attempt = attempt;
    }

    /**
     * get transaction manager.
     *
     * @return transaction manager, null if this transaction is not created by transaction manager
     */
    @Nullable
    public TsurugiTransactionManager getTransactionManager() {
        return this.txManager;
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
     * set transaction-begin-timeout
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setBeginTimeout(long time, TimeUnit unit) {
        setBeginTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set transaction-begin-timeout
     *
     * @param timeout time
     */
    public void setBeginTimeout(TgTimeValue timeout) {
        beginTimeout.set(timeout);
    }

    /**
     * set transaction-commit-timeout
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setCommitTimeout(long time, TimeUnit unit) {
        setCommitTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set transaction-commit-timeout
     *
     * @param timeout time
     */
    public void setCommitTimeout(TgTimeValue timeout) {
        commitTimeout.set(timeout);
    }

    /**
     * set transaction-rollback-timeout
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setRollbackTimeout(long time, TimeUnit unit) {
        setRollbackTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set transaction-rollback-timeout
     *
     * @param timeout time
     */
    public void setRollbackTimeout(TgTimeValue timeout) {
        rollbackTimeout.set(timeout);
    }

    /**
     * set transaction-close-timeout
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setCloseTimeout(long time, TimeUnit unit) {
        setCloseTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set transaction-close-timeout
     *
     * @param timeout time
     */
    public void setCloseTimeout(TgTimeValue timeout) {
        closeTimeout.set(timeout);

        applyCloseTimeout();
    }

    // internal
    public final TgSessionInfo getSessionInfo() {
        return ownerSession.getSessionInfo();
    }

    // internal
//  @ThreadSafe
    public final synchronized Transaction getLowTransaction() throws IOException {
        this.calledGetLowTransaction = true;
        if (this.lowTransaction == null) {
            if (this.lowFutureException != null) {
                throw new TsurugiIOException(IceaxeErrorCode.TX_LOW_ERROR, lowFutureException);
            }

            LOG.trace("lowTransaction get start");
            try {
                this.lowTransaction = IceaxeIoUtil.getAndCloseFuture(lowTransactionFuture, beginTimeout);
            } catch (Throwable e) {
                this.lowFutureException = e;
                throw e;
            }
            LOG.trace("lowTransaction get end");

            this.lowTransactionFuture = null;
            applyCloseTimeout();
        }
        return this.lowTransaction;
    }

    /**
     * Provides transaction id that is unique to for the duration of the database server's lifetime
     *
     * @return the id String for this transaction
     * @throws IOException
     */
    public String getTransactionId() throws IOException {
        var transaction = getLowTransaction();
        return transaction.getTransactionId();
    }

    // execute statement

    /**
     * execute ddl.
     *
     * @param sql DDL
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public void executeDdl(String sql) throws IOException, TsurugiTransactionException {
        try (var ps = ownerSession.createPreparedStatement(sql)) {
            executeAndGetCount(ps);
        }
    }

    /**
     * execute query.
     *
     * @param <R> result type
     * @param ps  PreparedStatement
     * @return ResultSet ({@link java.io.Closeable})
     * @throws IOException
     * @throws TsurugiTransactionException
     * @see #executeForEach(TsurugiPreparedStatementQuery0, TsurugiTransactionConsumer)
     * @see #executeAndFindRecord(TsurugiPreparedStatementQuery0)
     * @see #executeAndGetList(TsurugiPreparedStatementQuery0)
     */
    public <R> TsurugiResultSet<R> executeQuery(TsurugiPreparedStatementQuery0<R> ps) throws IOException, TsurugiTransactionException {
        return ps.execute(this);
    }

    /**
     * execute query.
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param ps        PreparedStatement
     * @param parameter SQL parameter
     * @return ResultSet ({@link java.io.Closeable})
     * @throws IOException
     * @throws TsurugiTransactionException
     * @see #executeForEach(TsurugiPreparedStatementQuery1, P, TsurugiTransactionConsumer)
     * @see #executeAndFindRecord(TsurugiPreparedStatementQuery1, P)
     * @see #executeAndGetList(TsurugiPreparedStatementQuery1, P)
     */
    public <P, R> TsurugiResultSet<R> executeQuery(TsurugiPreparedStatementQuery1<P, R> ps, P parameter) throws IOException, TsurugiTransactionException {
        return ps.execute(this, parameter);
    }

    /**
     * execute statement.
     *
     * @param ps PreparedStatement
     * @return result ({@link java.io.Closeable})
     * @throws IOException
     * @throws TsurugiTransactionException
     * @see #executeAndGetCount(TsurugiPreparedStatementUpdate0)
     */
    public TsurugiResultCount executeStatement(TsurugiPreparedStatementUpdate0 ps) throws IOException, TsurugiTransactionException {
        return ps.execute(this);
    }

    /**
     * execute statement.
     *
     * @param <P>       parameter type
     * @param ps        PreparedStatement
     * @param parameter SQL parameter
     * @return result ({@link java.io.Closeable})
     * @throws IOException
     * @throws TsurugiTransactionException
     * @see #executeAndGetCount(TsurugiPreparedStatementUpdate1, P)
     */
    public <P> TsurugiResultCount executeStatement(TsurugiPreparedStatementUpdate1<P> ps, P parameter) throws IOException, TsurugiTransactionException {
        return ps.execute(this, parameter);
    }

    /**
     * execute query.
     *
     * @param <R>       result type
     * @param ps        PreparedStatement
     * @param parameter SQL parameter
     * @param action    The action to be performed for each record
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public <R> void executeForEach(TsurugiPreparedStatementQuery0<R> ps, TsurugiTransactionConsumer<R> action) throws IOException, TsurugiTransactionException {
        try (var rs = ps.execute(this)) {
            for (var i = rs.iterator(); i.hasNext();) {
                R record = i.next();
                action.accept(record);
            }
        } catch (UncheckedIOException e) {
            throw e.getCause();
        } catch (TsurugiTransactionRuntimeException e) {
            throw e.getCause();
        }
    }

    /**
     * execute query.
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param ps        PreparedStatement
     * @param parameter SQL parameter
     * @param action    The action to be performed for each record
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public <P, R> void executeForEach(TsurugiPreparedStatementQuery1<P, R> ps, P parameter, TsurugiTransactionConsumer<R> action) throws IOException, TsurugiTransactionException {
        try (var rs = ps.execute(this, parameter)) {
            for (var i = rs.iterator(); i.hasNext();) {
                R record = i.next();
                action.accept(record);
            }
        } catch (UncheckedIOException e) {
            throw e.getCause();
        } catch (TsurugiTransactionRuntimeException e) {
            throw e.getCause();
        }
    }

    /**
     * execute query.
     *
     * @param <R> result type
     * @param ps  PreparedStatement
     * @return record
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public <R> Optional<R> executeAndFindRecord(TsurugiPreparedStatementQuery0<R> ps) throws IOException, TsurugiTransactionException {
        try (var rs = ps.execute(this)) {
            return rs.findRecord();
        }
    }

    /**
     * execute query.
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param ps        PreparedStatement
     * @param parameter SQL parameter
     * @return record
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public <P, R> Optional<R> executeAndFindRecord(TsurugiPreparedStatementQuery1<P, R> ps, P parameter) throws IOException, TsurugiTransactionException {
        try (var rs = ps.execute(this, parameter)) {
            return rs.findRecord();
        }
    }

    /**
     * execute query.
     *
     * @param <R> result type
     * @param ps  PreparedStatement
     * @return list of record
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public <R> List<R> executeAndGetList(TsurugiPreparedStatementQuery0<R> ps) throws IOException, TsurugiTransactionException {
        try (var rs = ps.execute(this)) {
            return rs.getRecordList();
        }
    }

    /**
     * execute query.
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param ps        PreparedStatement
     * @param parameter SQL parameter
     * @return list of record
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public <P, R> List<R> executeAndGetList(TsurugiPreparedStatementQuery1<P, R> ps, P parameter) throws IOException, TsurugiTransactionException {
        try (var rs = ps.execute(this, parameter)) {
            return rs.getRecordList();
        }
    }

    /**
     * execute statement.
     *
     * @param ps PreparedStatement
     * @return row count
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public int executeAndGetCount(TsurugiPreparedStatementUpdate0 ps) throws IOException, TsurugiTransactionException {
        try (var rc = ps.execute(this)) {
            return rc.getUpdateCount();
        }
    }

    /**
     * execute statement.
     *
     * @param <P>       parameter type
     * @param ps        PreparedStatement
     * @param parameter SQL parameter
     * @return row count
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public <P> int executeAndGetCount(TsurugiPreparedStatementUpdate1<P> ps, P parameter) throws IOException, TsurugiTransactionException {
        try (var rc = ps.execute(this, parameter)) {
            return rc.getUpdateCount();
        }
    }

    // internal
    @FunctionalInterface
    public interface LowTransactionTask<R> {
        R run(Transaction lowTransaction) throws IOException;
    }

    // internal
    public <R> R executeLow(LowTransactionTask<R> task) throws IOException {
        checkClose();
        return task.run(getLowTransaction());
    }

    // commit, rollback

    /**
     * add before-commit listener
     *
     * @param listener listener
     */
    public void addBeforeCommitListener(Consumer<TsurugiTransaction> listener) {
        if (this.beforeCommitListenerList == null) {
            this.beforeCommitListenerList = new ArrayList<>();
        }
        beforeCommitListenerList.add(listener);
    }

    /**
     * add commit listener
     *
     * @param listener listener
     */
    public void addCommitListener(Consumer<TsurugiTransaction> listener) {
        if (this.commitListenerList == null) {
            this.commitListenerList = new ArrayList<>();
        }
        commitListenerList.add(listener);
    }

    /**
     * add rollback listener
     *
     * @param listener listener
     */
    public void addRollbackListener(Consumer<TsurugiTransaction> listener) {
        if (this.rollbackListenerList == null) {
            this.rollbackListenerList = new ArrayList<>();
        }
        rollbackListenerList.add(listener);
    }

    /**
     * Whether transaction is available
     *
     * @return true: available
     * @throws IOException
     */
//  @ThreadSafe
    public final synchronized boolean available() throws IOException {
        if (isClosed()) {
            return false;
        }
        if (!this.calledGetLowTransaction) {
            getLowTransaction();
        }
        return this.lowTransaction != null;
    }

    /**
     * do commit
     *
     * @param commitType commit type
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public synchronized void commit(TgCommitType commitType) throws IOException, TsurugiTransactionException {
        checkClose();
        if (this.committed) {
            return;
        }
        if (this.rollbacked) {
            throw new IllegalStateException("rollback has already been called");
        }

        LOG.trace("transaction commit start. commitType={}", commitType);
        if (beforeCommitListenerList != null) {
            for (var listener : beforeCommitListenerList) {
                listener.accept(this);
            }
        }

        closeableSet.closeInTransaction();
        var lowCommitStatus = commitType.getLowCommitStatus();
        finish(lowTx -> lowTx.commit(lowCommitStatus), commitTimeout);
        LOG.trace("transaction commit end");
        this.committed = true;

        if (commitListenerList != null) {
            for (var listener : commitListenerList) {
                listener.accept(this);
            }
        }
    }

    /**
     * do rollback
     *
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public synchronized void rollback() throws IOException, TsurugiTransactionException {
        checkClose();
        if (this.committed || this.rollbacked) {
            return;
        }

        LOG.trace("transaction rollback start");
        closeableSet.closeInTransaction();
        finish(Transaction::rollback, rollbackTimeout);
        LOG.trace("transaction rollback end");
        this.rollbacked = true;

        if (rollbackListenerList != null) {
            for (var listener : rollbackListenerList) {
                listener.accept(this);
            }
        }
    }

    protected void finish(IoFunction<Transaction, FutureResponse<Void>> finisher, IceaxeTimeout timeout) throws IOException, TsurugiTransactionException {
        var transaction = getLowTransaction();
        var lowResultFuture = finisher.apply(transaction);
        closeTimeout.apply(lowResultFuture);
        IceaxeIoUtil.getAndCloseFutureInTransaction(lowResultFuture, timeout);
    }

    /**
     * get committed
     *
     * @return true: committed
     */
    public synchronized boolean isCommitted() {
        return this.committed;
    }

    /**
     * get rollbacked
     *
     * @return true: rollbacked
     */
    public synchronized boolean isRollbacked() {
        return this.rollbacked;
    }

    // internal
    public void addChild(AutoCloseable closeable) throws IOException {
        checkClose();
        closeableSet.add(closeable);
    }

    // internal
    public void removeChild(AutoCloseable closeable) {
        closeableSet.remove(closeable);
    }

    @Override
    public void close() throws IOException {
        this.closed = true;

//      if (!(this.committed || this.rollbacked)) {
        // commitやrollbackに失敗してもcloseは呼ばれるので、ここでIllegalStateException等を発生させるのは良くない
//      }

        if (LOG.isTraceEnabled()) {
            LOG.trace("transaction close start. committed={}, rollbacked={}", committed, rollbacked);
        }
        IceaxeIoUtil.close(closeableSet, () -> {
            // not try-finally
            IceaxeIoUtil.close(lowTransaction, lowTransactionFuture);
            ownerSession.removeChild(this);
        });
        LOG.trace("transaction close end");
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

    protected void checkClose() throws IOException {
        if (isClosed()) {
            throw new TsurugiIOException(IceaxeErrorCode.TX_ALREADY_CLOSED);
        }
    }
}
