package com.tsurugidb.iceaxe.transaction;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatement;
import com.tsurugidb.iceaxe.util.TgTimeValue;

/**
 * Tsurugi Transaction Manager
 * <p>
 * MT safe (excluding setTimeout)
 * </p>
 */
public class TsurugiTransactionManager {

    private final TsurugiSession ownerSession;
    private final List<TgTransactionOption> defaultTransactionOptionList;

    private TgTimeValue beginTimeout;
    private TgTimeValue commitTimeout;
    private TgTimeValue rollbackTimeout;

    // internal
    public TsurugiTransactionManager(TsurugiSession session, List<TgTransactionOption> defaultTransactionOptionList) {
        this.ownerSession = session;
        this.defaultTransactionOptionList = defaultTransactionOptionList;
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
        this.beginTimeout = timeout;
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
        this.commitTimeout = timeout;
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
        this.rollbackTimeout = timeout;
    }

    @FunctionalInterface
    public interface TsurugiTransactionConsumer {
        public void accept(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException;
    }

    /**
     * execute transaction
     * 
     * @param action action
     * @throws IOException
     * @see TsurugiPreparedStatement
     */
    public void execute(TsurugiTransactionConsumer action) throws IOException {
        execute(defaultTransactionOptionList, action);
    }

    /**
     * execute transaction
     * 
     * @param transactionOptionList transaction option
     * @param action                action
     * @throws IOException
     * @see TsurugiPreparedStatement
     */
    public void execute(List<TgTransactionOption> transactionOptionList, TsurugiTransactionConsumer action) throws IOException {
        if (action == null) {
            throw new IllegalArgumentException("action is not specified");
        }
        execute(transactionOptionList, transaction -> {
            action.accept(transaction);
            return null;
        });
    }

    @FunctionalInterface
    public interface TsurugiTransactionFuntion<R> {
        public R apply(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException;
    }

    /**
     * execute transaction
     * 
     * @param <R>                   return type
     * @param transactionOptionList transaction option
     * @param action                action
     * @return return value (null if transaction is rollbacked)
     * @throws IOException
     * @see TsurugiPreparedStatement
     */
    public <R> R execute(TsurugiTransactionFuntion<R> action) throws IOException {
        var transactionOptionList = this.defaultTransactionOptionList;
        if (transactionOptionList == null || transactionOptionList.isEmpty()) {
            throw new IllegalStateException("defaultTransactionOptionList is not specified");
        }
        return execute(transactionOptionList, action);
    }

    /**
     * execute transaction
     * 
     * @param <R>                   return type
     * @param transactionOptionList transaction option
     * @param action                action
     * @return return value (null if transaction is rollbacked)
     * @throws IOException
     * @see TsurugiPreparedStatement
     */
    public <R> R execute(List<TgTransactionOption> transactionOptionList, TsurugiTransactionFuntion<R> action) throws IOException {
        if (transactionOptionList == null || transactionOptionList.isEmpty()) {
            throw new IllegalArgumentException("transactionOptionList is not specified");
        }
        if (action == null) {
            throw new IllegalArgumentException("action is not specified");
        }
        for (var option : transactionOptionList) {
            try (var transaction = ownerSession.createTransaction(option)) {
                initializeTransaction(transaction);

                boolean doRollback = true;
                try {
                    var r = action.apply(transaction);
                    if (transaction.isRollbacked()) {
                        doRollback = false;
                        return null;
                    }
                    transaction.commit();
                    doRollback = false;
                    return r;
                } catch (TsurugiTransactionException e) {
                    if (isRetryable(e)) {
                        // リトライ可能なabortの場合でもrollbackは呼ぶ（doRollback=true）
                        continue;
                    }
                    throw new IOException(e);
                } catch (TsurugiTransactionRuntimeException e) {
                    var c = e.getCause();
                    if (isRetryable(c)) {
                        // リトライ可能なabortの場合でもrollbackは呼ぶ（doRollback=true）
                        continue;
                    }
                    throw new IOException(e);
                } catch (Exception e) {
                    if (isRetryable(e)) {
                        // リトライ可能なabortの場合でもrollbackは呼ぶ（doRollback=true）
                        continue;
                    }
                    throw e;
                } finally {
                    if (doRollback) {
                        try {
                            transaction.rollback();
                        } catch (TsurugiTransactionException e) {
                            throw new IOException(e);
                        }
                    }
                }
            }
        }
        throw new IOException("transaction retry over");
    }

    protected void initializeTransaction(TsurugiTransaction transaction) {
        if (beginTimeout != null) {
            transaction.setBeginTimeout(beginTimeout);
        }
        if (commitTimeout != null) {
            transaction.setCommitTimeout(commitTimeout);
        }
        if (rollbackTimeout != null) {
            transaction.setRollbackTimeout(rollbackTimeout);
        }
    }

    protected boolean isRetryable(Exception e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof TsurugiTransactionException) {
                return isRetryable((TsurugiTransactionException) t);
            }
        }
        return false;
    }

    protected boolean isRetryable(TsurugiTransactionException e) {
        return e.isRetryable();
    }
}
