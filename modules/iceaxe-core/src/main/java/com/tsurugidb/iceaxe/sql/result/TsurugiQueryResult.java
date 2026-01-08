/*
 * Copyright 2023-2026 Project Tsurugi.
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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeTimeoutIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.result.event.TsurugiQueryResultEventListener;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.InterruptedRuntimeException;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.iceaxe.util.function.TsurugiTransactionConsumer;
import com.tsurugidb.iceaxe.util.function.TsurugiTransactionConsumerWithRowNumber;
import com.tsurugidb.sql.proto.SqlCommon.Column;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
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

    private final TgResultMapping<R> resultMapping;
    private final IceaxeConvertUtil convertUtil;
    private FutureResponse<ResultSet> lowResultSetFuture;
    private ResultSet lowResultSet;
    private TgTimeValue fetchTimeout;
    private List<TsurugiQueryResultEventListener<R>> eventListenerList = null;
    private int readCount = 0;
    private TsurugiResultRecord record;
    private Optional<Boolean> hasNextRow = Optional.empty();
    private boolean checkResultOnClose = true;
    private boolean calledEndEvent = false;

    /**
     * Creates a new instance.
     * <p>
     * Call {@link #initialize(FutureResponse)} after construct.
     * </p>
     *
     * @param sqlExecuteId      iceaxe SQL executeId
     * @param transaction       transaction
     * @param ps                SQL definition
     * @param parameter         SQL parameter
     * @param resultMapping     result mapping
     * @param convertUtil       convert type utility
     * @param afterCloseableSet Closeable set for execute finished
     */
    @IceaxeInternal
    public TsurugiQueryResult(int sqlExecuteId, TsurugiTransaction transaction, TsurugiSql ps, Object parameter, TgResultMapping<R> resultMapping, IceaxeConvertUtil convertUtil,
            IceaxeCloseableSet afterCloseableSet) {
        super(sqlExecuteId, transaction, ps, parameter, afterCloseableSet, TgTimeoutKey.RS_CONNECT, TgTimeoutKey.RS_CLOSE);
        this.resultMapping = resultMapping;
        this.convertUtil = convertUtil;

        var sessionOption = transaction.getSession().getSessionOption();
        this.fetchTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.RS_FETCH).get();
    }

    /**
     * initialize.
     * <p>
     * Call this method only once after construct.
     * </p>
     *
     * @param lowResultSetFuture future of ResultSet
     * @throws IOException if transaction already closed
     * @since 1.3.0
     */
    @IceaxeInternal
    public void initialize(FutureResponse<ResultSet> lowResultSetFuture) throws IOException {
        if (this.lowResultSetFuture != null || this.lowResultSet != null) {
            throw new IllegalStateException("initialize() is already called");
        }

        this.lowResultSetFuture = Objects.requireNonNull(lowResultSetFuture);

        try {
            super.initialize();
        } catch (Throwable e) {
            var log = LoggerFactory.getLogger(getClass());
            log.trace("TsurugiQueryResult.initialize close start", e);
            try {
                IceaxeIoUtil.closeInTransaction(closeTimeout.getNanos(), IceaxeErrorCode.RS_CLOSE_TIMEOUT, IceaxeErrorCode.RS_CLOSE_ERROR, //
                        lowResultSetFuture);
            } catch (Throwable c) {
                e.addSuppressed(c);
            }
            this.checkResultOnClose = false;
            log.trace("TsurugiQueryResult.initialize close end");
            throw e;
        }
    }

    /**
     * set fetch-timeout.
     *
     * @param time timeout time
     * @param unit timeout unit
     * @since 1.9.0
     */
    public void setFetchTimeout(long time, TimeUnit unit) {
        this.fetchTimeout = new TgTimeValue(time, unit);
        applyFetchTimeout();
    }

    /**
     * set fetch-timeout.
     *
     * @param timeout time
     * @since 1.9.0
     */
    public void setFetchTimeout(TgTimeValue timeout) {
        this.fetchTimeout = Objects.requireNonNull(timeout);
        applyFetchTimeout();
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

    /**
     * find event listener.
     *
     * @param predicate predicate for event listener
     * @return event listener
     * @since 1.3.0
     */
    public Optional<TsurugiQueryResultEventListener<R>> findEventListener(Predicate<TsurugiQueryResultEventListener<R>> predicate) {
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

    private void event(Throwable occurred, Consumer<TsurugiQueryResultEventListener<R>> action) {
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
     * get {@link ResultSet}.
     *
     * @return SQL result set
     * @throws IOException                 if an I/O error occurs while retrieving result set
     * @throws InterruptedException        if interrupted while retrieving result set
     * @throws TsurugiTransactionException if server error occurs while retrieving result set
     */
    @IceaxeInternal
    public final synchronized ResultSet getLowResultSet() throws IOException, InterruptedException, TsurugiTransactionException {
        this.checkResultOnClose = false;
        if (this.lowResultSet == null) {
            if (this.lowResultSetFuture == null) {
                throw new IllegalStateException("initialize() is not called");
            }

            LOG.trace("lowResultSet get start");
            try {
                this.lowResultSet = IceaxeIoUtil.getAndCloseFutureInTransaction(lowResultSetFuture, //
                        connectTimeout, IceaxeErrorCode.RS_CONNECT_TIMEOUT, //
                        IceaxeErrorCode.RS_CLOSE_TIMEOUT);
            } catch (TsurugiTransactionException e) {
                fillToTsurugiException(e);
                throw e;
            }
            LOG.trace("lowResultSet get end");

            this.lowResultSetFuture = null;

            applyFetchTimeout();
        }
        return this.lowResultSet;
    }

    private void applyFetchTimeout() {
        var lowResultSet = this.lowResultSet;
        if (lowResultSet != null) {
            var time = this.fetchTimeout;
            lowResultSet.setTimeout(time.value(), time.unit());
        }
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
        } catch (ResponseTimeoutException e) {
            event(e, listener -> listener.readException(this, e));
            throw new IceaxeTimeoutIOException(IceaxeErrorCode.RS_NEXT_ROW_TIMEOUT, e);
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
            var lowColumnList = getLowColumnList(this, getLowResultSet());
            return IceaxeResultNameList.toNameList(lowColumnList);
        } catch (Throwable e) {
            event(e, listener -> listener.readException(this, e));
            throw e;
        }
    }

    static List<? extends Column> getLowColumnList(TsurugiQueryResult<?> rs, ResultSet lowResultSet) throws IOException, InterruptedException, TsurugiTransactionException {
        try {
            var lowMetadata = lowResultSet.getMetadata();
            return lowMetadata.getColumns();
        } catch (ServerException e) {
            throw rs.fillToTsurugiException(new TsurugiTransactionException(e));
        }
    }

    // read

    /**
     * Get next record.
     *
     * @return record. {@code empty} if end of record.
     * @throws IOException                 if an I/O error occurs while retrieving record
     * @throws InterruptedException        if interrupted while retrieving record
     * @throws TsurugiTransactionException if server error occurs while retrieving record
     * @since 1.9.0
     */
    public Optional<R> nextRecord() throws IOException, InterruptedException, TsurugiTransactionException {
        if (nextLowRecord()) {
            var record = getRecord();
            record.reset();
            R result = convertRecord(record);
            event(null, listener -> listener.readRecord(this, result));
            return Optional.of(result);
        }
        return Optional.empty();
    }

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
     * Performs the given action for each record.
     *
     * @param action The action to be performed for each record
     * @throws IOException                 if an I/O error occurs while retrieving record
     * @throws InterruptedException        if interrupted while retrieving record
     * @throws TsurugiTransactionException if server error occurs while retrieving record
     * @since 1.9.0
     */
    public void whileEach(TsurugiTransactionConsumerWithRowNumber<R> action) throws IOException, InterruptedException, TsurugiTransactionException {
        var record = getRecord();
        for (int i = 0; nextLowRecord(); i++) {
            record.reset();
            R result = convertRecord(record);
            event(null, listener -> listener.readRecord(this, result));
            action.accept(i, result);
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
        close(closeTimeout.getNanos());
    }

    @Override
    public void close(long timeoutNanos) throws IOException, InterruptedException, TsurugiTransactionException {
        LOG.trace("queryResult close start");

        Throwable occurred = null;
        try {
            if (enableCheckResultOnClose()) {
                if (this.checkResultOnClose) {
                    getLowResultSet();
                }
            }
            callEndEvent();
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            try {
                // not try-finally
                IceaxeIoUtil.closeInTransaction(timeoutNanos, IceaxeErrorCode.RS_CLOSE_TIMEOUT, IceaxeErrorCode.RS_CLOSE_ERROR, //
                        lowResultSet, lowResultSetFuture);
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

        LOG.trace("queryResult close end");
    }
}
