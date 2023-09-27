package com.tsurugidb.iceaxe.sql.result;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.result.event.TsurugiQueryResultEventListener;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.InterruptedRuntimeException;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.iceaxe.util.function.TsurugiTransactionConsumer;
import com.tsurugidb.sql.proto.SqlCommon.Column;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi SQL Result for query.
 *
 * @param <R> result type
 * @see TsurugiSqlQuery#execute(TsurugiTransaction)
 * @see TsurugiSqlPreparedQuery#execute(TsurugiTransaction, Object)
 */
@NotThreadSafe
public class TsurugiQueryResult<R> extends TsurugiSqlResult implements Iterable<R> {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiQueryResult.class);

    private FutureResponse<ResultSet> lowResultSetFuture;
    private ResultSet lowResultSet;
    private boolean calledGetLowResultSet = false;
    private final TgResultMapping<R> resultMapping;
    private final IceaxeConvertUtil convertUtil;
    private final IceaxeTimeout connectTimeout;
    private final IceaxeTimeout closeTimeout;
    private List<TsurugiQueryResultEventListener<R>> eventListenerList = null;
    private int readCount = 0;
    private TsurugiResultRecord record;
    private Optional<Boolean> hasNextRow = Optional.empty();
    private boolean calledEndEvent = false;

    /**
     * Creates a new instance.
     *
     * @param sqlExecuteId       iceaxe SQL executeId
     * @param transaction        transaction
     * @param ps                 SQL definition
     * @param parameter          SQL parameter
     * @param lowResultSetFuture future of ResultSet
     * @param resultMapping      result mapping
     * @param convertUtil        convert type utility
     * @throws IOException if an I/O error occurs while disposing the resources
     */
    @IceaxeInternal
    public TsurugiQueryResult(int sqlExecuteId, TsurugiTransaction transaction, TsurugiSql ps, Object parameter, FutureResponse<ResultSet> lowResultSetFuture, TgResultMapping<R> resultMapping,
            IceaxeConvertUtil convertUtil) throws IOException {
        super(sqlExecuteId, transaction, ps, parameter);
        this.lowResultSetFuture = lowResultSetFuture;
        this.resultMapping = resultMapping;
        this.convertUtil = convertUtil;

        var sessionOption = transaction.getSessionOption();
        this.connectTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.RS_CONNECT);
        this.closeTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.RS_CLOSE);

        applyCloseTimeout();
    }

    private void applyCloseTimeout() {
        closeTimeout.apply(lowResultSet);
        closeTimeout.apply(lowResultSetFuture);
    }

    /**
     * set ResetSet-timeout.
     *
     * @param time time value
     * @param unit time unit
     */
    public void setRsConnectTimeout(long time, TimeUnit unit) {
        setRsConnectTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set ResetSet-timeout.
     *
     * @param timeout time
     */
    public void setRsConnectTimeout(TgTimeValue timeout) {
        connectTimeout.set(timeout);
    }

    /**
     * set ResetSet-close-timeout.
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setRsCloseTimeout(long time, TimeUnit unit) {
        setRsCloseTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set ResetSet-close-timeout.
     *
     * @param timeout time
     */
    public void setRsCloseTimeout(TgTimeValue timeout) {
        closeTimeout.set(timeout);

        applyCloseTimeout();
    }

    /**
     * add event listener.
     *
     * @param listener event listener
     * @return this
     */
    public TsurugiSqlResult addEventListener(TsurugiQueryResultEventListener<R> listener) {
        if (this.eventListenerList == null) {
            this.eventListenerList = new ArrayList<>();
        }
        eventListenerList.add(listener);
        return this;
    }

    private void event(Throwable occurred, Consumer<TsurugiQueryResultEventListener<R>> action) {
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

    /**
     * get {@link ResultSet}.
     *
     * @return SQL result set
     * @throws IOException                 if an I/O error occurs while retrieving result set
     * @throws InterruptedException        if interrupted while retrieving result set
     * @throws TsurugiTransactionException if server error occurs while retrieving result set
     */
    @IceaxeInternal
    public final synchronized ResultSet getLowResultSet() throws IOException, InterruptedException, TsurugiTransactionException {
        this.calledGetLowResultSet = true;
        if (this.lowResultSet == null) {
            LOG.trace("lowResultSet get start");
            try {
                this.lowResultSet = IceaxeIoUtil.getAndCloseFutureInTransaction(lowResultSetFuture, connectTimeout);
            } catch (TsurugiTransactionException e) {
                fillToTsurugiException(e);
                throw e;
            }
            LOG.trace("lowResultSet get end");

            this.lowResultSetFuture = null;
            applyCloseTimeout();
        }
        return this.lowResultSet;
    }

    /**
     * Advances to the next record.
     *
     * @return {@code false} if there are no more rows
     * @throws IOException                 if an I/O error occurs while retrieving result set
     * @throws InterruptedException        if interrupted while retrieving result set
     * @throws TsurugiTransactionException if server error occurs while retrieving result set
     */
    protected boolean nextLowRecord() throws IOException, InterruptedException, TsurugiTransactionException {
        boolean exists;
        try {
            var lowResultSet = getLowResultSet();
            LOG.trace("nextLowRecord start");
            exists = lowResultSet.nextRow();
            if (LOG.isTraceEnabled()) {
                LOG.trace("nextLowRecord end. exists={}", exists);
            }
        } catch (ServerException e) {
            event(e, listener -> listener.readException(this, e));
            throw fillToTsurugiException(new TsurugiTransactionException(e));
        } catch (Throwable e) {
            event(e, listener -> listener.readException(this, e));
            throw e;
        }

        if (this.hasNextRow.isEmpty() || hasNextRow.get().booleanValue() != exists) {
            this.hasNextRow = Optional.of(exists);
        }
        if (!exists) {
            callEndEvent();
        }
        return exists;
    }

    private void callEndEvent() {
        if (this.calledEndEvent) {
            return;
        }
        this.calledEndEvent = true;

        event(null, listener -> listener.endResult(this));
    }

    /**
     * get hasNextRow.
     *
     * @return hasNextRow
     */
    public Optional<Boolean> getHasNextRow() {
        return this.hasNextRow;
    }

    /**
     * get record.
     *
     * @return record
     * @throws IOException                 if an I/O error occurs while retrieving result set
     * @throws InterruptedException        if interrupted while retrieving result set
     * @throws TsurugiTransactionException if server error occurs while retrieving result set
     */
    protected TsurugiResultRecord getRecord() throws IOException, InterruptedException, TsurugiTransactionException {
        if (this.record == null) {
            try {
                var lowResultSet = getLowResultSet();
                this.record = new TsurugiResultRecord(this, lowResultSet, convertUtil);
            } catch (Throwable e) {
                event(e, listener -> listener.readException(this, e));
                throw e;
            }
        }
        return this.record;
    }

    /**
     * convert record to R.
     *
     * @param record record
     * @return record(R type)
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    protected R convertRecord(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        R result;
        try {
            result = resultMapping.convert(record);
        } catch (TsurugiTransactionException e) {
            event(e, listener -> listener.readException(this, e));
            fillToTsurugiException(e);
            throw e;
        } catch (Throwable e) {
            event(e, listener -> listener.readException(this, e));
            throw e;
        }
        this.readCount++;
        return result;
    }

    /**
     * get number of read.
     *
     * @return number of read
     */
    public int getReadCount() {
        return this.readCount;
    }

    // column name

    /**
     * get name list.
     *
     * @return list of column name
     * @throws IOException                 if an I/O error occurs while retrieving metadata
     * @throws InterruptedException        if interrupted while retrieving metadata
     * @throws TsurugiTransactionException if server error occurs while retrieving metadata
     */
    public List<String> getNameList() throws IOException, InterruptedException, TsurugiTransactionException {
        try {
            return getNameList(this, getLowResultSet());
        } catch (Throwable e) {
            event(e, listener -> listener.readException(this, e));
            throw e;
        }
    }

    static List<String> getNameList(TsurugiQueryResult<?> rs, ResultSet lowResultSet) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowColumnList = getLowColumnList(rs, lowResultSet);
        var size = lowColumnList.size();
        var list = new ArrayList<String>(size);
        int i = 0;
        for (var lowColumn : lowColumnList) {
            var name = getColumnName(lowColumn, i++);
            list.add(name);
        }
        return list;
    }

    static List<? extends Column> getLowColumnList(TsurugiQueryResult<?> rs, ResultSet lowResultSet) throws IOException, InterruptedException, TsurugiTransactionException {
        try {
            var lowMetadata = lowResultSet.getMetadata();
            return lowMetadata.getColumns();
        } catch (ServerException e) {
            throw rs.fillToTsurugiException(new TsurugiTransactionException(e));
        }
    }

    static String getColumnName(Column lowColumn, int index) {
        var lowName = lowColumn.getName();
        if (lowName == null || lowName.isEmpty()) {
            return "@#" + index;
        }
        return lowName;
    }

    // read

    /**
     * Performs the given action for each record.
     *
     * @param action The action to be performed for each record
     * @throws IOException                 if an I/O error occurs while retrieving record
     * @throws InterruptedException        if interrupted while retrieving record
     * @throws TsurugiTransactionException if server error occurs while retrieving record
     */
    public void whileEach(TsurugiTransactionConsumer<R> action) throws IOException, InterruptedException, TsurugiTransactionException {
        var record = getRecord();
        while (nextLowRecord()) {
            record.reset();
            R result = convertRecord(record);
            event(null, listener -> listener.readRecord(this, result));
            action.accept(result);
        }
    }

    /**
     * get record list.
     *
     * @return list of record
     * @throws IOException                 if an I/O error occurs while retrieving record
     * @throws InterruptedException        if interrupted while retrieving record
     * @throws TsurugiTransactionException if server error occurs while retrieving record
     */
    public List<R> getRecordList() throws IOException, InterruptedException, TsurugiTransactionException {
        var list = new ArrayList<R>();
        var record = getRecord();
        while (nextLowRecord()) {
            record.reset();
            R result = convertRecord(record);
            event(null, listener -> listener.readRecord(this, result));
            list.add(result);
        }
        return list;
    }

    /**
     * get one record.
     *
     * @return record
     * @throws IOException                 if an I/O error occurs while retrieving record
     * @throws InterruptedException        if interrupted while retrieving record
     * @throws TsurugiTransactionException if server error occurs while retrieving record
     */
    public Optional<R> findRecord() throws IOException, InterruptedException, TsurugiTransactionException {
        if (nextLowRecord()) {
            var record = getRecord();
            record.reset();
            R result = convertRecord(record);
            event(null, listener -> listener.readRecord(this, result));
            return Optional.ofNullable(result);
        } else {
            return Optional.empty();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws UncheckedIOException               if an I/O error occurs while retrieving record
     * @throws InterruptedRuntimeException        if interrupted while retrieving record
     * @throws TsurugiTransactionRuntimeException if server error occurs while retrieving record
     */
    @Override
    public Iterator<R> iterator() {
        try {
            var record = getRecord();
            return new TsurugiQueryResultIterator(record);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } catch (TsurugiTransactionException e) {
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    /**
     * Iterator for {@link TsurugiQueryResult}.
     */
    protected class TsurugiQueryResultIterator implements Iterator<R> {
        private final TsurugiResultRecord record;
        private boolean moveNext = true;
        private boolean hasNext;

        /**
         * Creates a new instance.
         *
         * @param record record
         */
        public TsurugiQueryResultIterator(TsurugiResultRecord record) {
            this.record = record;
        }

        /**
         * move next record.
         *
         * @throws UncheckedIOException               if an I/O error occurs while retrieving record
         * @throws InterruptedRuntimeException        if interrupted while retrieving record
         * @throws TsurugiTransactionRuntimeException if server error occurs while retrieving record
         */
        protected void moveNext() {
            if (this.moveNext) {
                try {
                    this.hasNext = nextLowRecord();
                } catch (IOException e) {
                    throw new UncheckedIOException(e.getMessage(), e);
                } catch (InterruptedException e) {
                    throw new InterruptedRuntimeException(e);
                } catch (TsurugiTransactionException e) {
                    throw new TsurugiTransactionRuntimeException(e);
                } finally {
                    record.reset();
                }
                this.moveNext = false;
            }
        }

        /**
         * {@inheritDoc}
         *
         * @throws UncheckedIOException               if an I/O error occurs while retrieving record
         * @throws InterruptedRuntimeException        if interrupted while retrieving record
         * @throws TsurugiTransactionRuntimeException if server error occurs while retrieving record
         */
        @Override
        public boolean hasNext() {
            moveNext();
            return this.hasNext;
        }

        /**
         * {@inheritDoc}
         *
         * @throws NoSuchElementException             if the iteration has no more elements
         * @throws UncheckedIOException               if an I/O error occurs while retrieving record
         * @throws InterruptedRuntimeException        if interrupted while retrieving record
         * @throws TsurugiTransactionRuntimeException if server error occurs while retrieving record
         */
        @Override
        public R next() {
            moveNext();
            if (!this.hasNext) {
                throw new NoSuchElementException();
            }

            R result;
            try {
                result = convertRecord(record);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            } catch (InterruptedException e) {
                throw new InterruptedRuntimeException(e);
            } catch (TsurugiTransactionException e) {
                throw new TsurugiTransactionRuntimeException(e);
            }
            event(null, listener -> listener.readRecord(TsurugiQueryResult.this, result));
            this.moveNext = true;
            return result;
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws UncheckedIOException               if an I/O error occurs while retrieving record
     * @throws InterruptedRuntimeException        if interrupted while retrieving record
     * @throws TsurugiTransactionRuntimeException if server error occurs while retrieving record
     * @see #whileEach(TsurugiTransactionConsumer)
     */
    @Override
    public void forEach(Consumer<? super R> action) {
        try {
            var record = getRecord();
            while (nextLowRecord()) {
                record.reset();
                R result = convertRecord(record);
                event(null, listener -> listener.readRecord(this, result));
                action.accept(result);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } catch (TsurugiTransactionException e) {
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    // close

    @Override
    public void close() throws IOException, InterruptedException, TsurugiTransactionException {
        LOG.trace("queryResult close start");

        Throwable occurred = null;
        try {
            if (!this.calledGetLowResultSet) {
                getLowResultSet();
            }
            callEndEvent();
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            try {
                // not try-finally
                IceaxeIoUtil.closeInTransaction(lowResultSet, lowResultSetFuture);
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
                event(occurred, listener -> listener.closeResult(this, finalOccurred));
            }
        }

        LOG.trace("queryResult close end");
    }
}
