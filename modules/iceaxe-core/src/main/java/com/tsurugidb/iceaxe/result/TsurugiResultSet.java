package com.tsurugidb.iceaxe.result;

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

import com.tsurugidb.iceaxe.result.event.TsurugiResultSetEventListener;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.sql.proto.SqlCommon.Column;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi Result Set for PreparedStatement
 *
 * @param <R> result type
 */
@NotThreadSafe
public class TsurugiResultSet<R> extends TsurugiResult implements Iterable<R> {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiResultSet.class);

    private FutureResponse<ResultSet> lowResultSetFuture;
    private ResultSet lowResultSet;
    private boolean calledGetLowResultSet = false;
    private final TgResultMapping<R> resultMapping;
    private final IceaxeConvertUtil convertUtil;
    private final IceaxeTimeout connectTimeout;
    private final IceaxeTimeout closeTimeout;
    private List<TsurugiResultSetEventListener<R>> eventListenerList = null;
    private int readCount = 0;
    private TsurugiResultRecord record;
    private Optional<Boolean> hasNextRow = Optional.empty();
    private boolean calledEndEvent = false;

    // internal
    public TsurugiResultSet(int sqlExecuteId, TsurugiTransaction transaction, FutureResponse<ResultSet> lowResultSetFuture, TgResultMapping<R> resultMapping, IceaxeConvertUtil convertUtil)
            throws IOException {
        super(sqlExecuteId, transaction);
        this.lowResultSetFuture = lowResultSetFuture;
        this.resultMapping = resultMapping;
        this.convertUtil = convertUtil;

        var info = transaction.getSessionInfo();
        this.connectTimeout = new IceaxeTimeout(info, TgTimeoutKey.RS_CONNECT);
        this.closeTimeout = new IceaxeTimeout(info, TgTimeoutKey.RS_CLOSE);

        applyCloseTimeout();
    }

    private void applyCloseTimeout() {
        closeTimeout.apply(lowResultSet);
        closeTimeout.apply(lowResultSetFuture);
    }

    /**
     * set ResetSet-timeout
     *
     * @param timeout time
     */
    public void setRsConnectTimeout(long time, TimeUnit unit) {
        setRsConnectTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set ResetSet-timeout
     *
     * @param timeout time
     */
    public void setRsConnectTimeout(TgTimeValue timeout) {
        connectTimeout.set(timeout);
    }

    /**
     * set ResetSet-close-timeout
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setRsCloseTimeout(long time, TimeUnit unit) {
        setRsCloseTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set ResetSet-close-timeout
     *
     * @param timeout time
     */
    public void setRsCloseTimeout(TgTimeValue timeout) {
        closeTimeout.set(timeout);

        applyCloseTimeout();
    }

    /**
     * add event listener
     *
     * @param listener event listener
     * @return this
     */
    public TsurugiResult addEventListener(TsurugiResultSetEventListener<R> listener) {
        if (this.eventListenerList == null) {
            this.eventListenerList = new ArrayList<>();
        }
        eventListenerList.add(listener);
        return this;
    }

    private void event(Throwable occurred, Consumer<TsurugiResultSetEventListener<R>> action) {
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

    protected final synchronized ResultSet getLowResultSet() throws IOException, TsurugiTransactionException {
        this.calledGetLowResultSet = true;
        if (this.lowResultSet == null) {
            LOG.trace("lowResultSet get start");
            this.lowResultSet = IceaxeIoUtil.getAndCloseFutureInTransaction(lowResultSetFuture, connectTimeout);
            LOG.trace("lowResultSet get end");

            this.lowResultSetFuture = null;
            applyCloseTimeout();
        }
        return this.lowResultSet;
    }

    protected boolean nextLowRecord() throws IOException, TsurugiTransactionException {
        try {
            var lowResultSet = getLowResultSet();
            LOG.trace("nextLowRecord start");
            boolean exists = lowResultSet.nextRow();
            if (LOG.isTraceEnabled()) {
                LOG.trace("nextLowRecord end. exists={}", exists);
            }
            if (this.hasNextRow.isEmpty() || hasNextRow.get().booleanValue() != exists) {
                this.hasNextRow = Optional.of(exists);
            }
            if (!exists) {
                callEndEvent();
            }
            return exists;
        } catch (ServerException e) {
            event(e, listener -> listener.readException(this, e));
            throw new TsurugiTransactionException(e);
        } catch (InterruptedException e) {
            event(e, listener -> listener.readException(this, e));
            throw new IOException(e.getMessage(), e);
        } catch (Throwable e) {
            event(e, listener -> listener.readException(this, e));
            throw e;
        }
    }

    private void callEndEvent() {
        if (this.calledEndEvent) {
            return;
        }
        this.calledEndEvent = true;

        event(null, listener -> listener.endResult(this));
    }

    /**
     * get hasNextRow
     *
     * @return hasNextRow
     */
    public Optional<Boolean> getHasNextRow() {
        return this.hasNextRow;
    }

    /**
     * get name list
     *
     * @return list of column name
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public List<String> getNameList() throws IOException, TsurugiTransactionException {
        try {
            return getNameList(getLowResultSet());
        } catch (Throwable e) {
            event(e, listener -> listener.readException(this, e));
            throw e;
        }
    }

    static List<String> getNameList(ResultSet lowResultSet) throws IOException, TsurugiTransactionException {
        var lowColumnList = getLowColumnList(lowResultSet);
        var size = lowColumnList.size();
        var list = new ArrayList<String>(size);
        int i = 0;
        for (var lowColumn : lowColumnList) {
            var name = getColumnName(lowColumn, i++);
            list.add(name);
        }
        return list;
    }

    static List<? extends Column> getLowColumnList(ResultSet lowResultSet) throws IOException, TsurugiTransactionException {
        try {
            var lowMetadata = lowResultSet.getMetadata();
            return lowMetadata.getColumns();
        } catch (ServerException e) {
            throw new TsurugiTransactionException(e);
        } catch (InterruptedException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    static String getColumnName(Column lowColumn, int index) {
        var lowName = lowColumn.getName();
        if (lowName == null || lowName.isEmpty()) {
            return "@#" + index;
        }
        return lowName;
    }

    /**
     * get one record
     *
     * @return record
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public Optional<R> findRecord() throws IOException, TsurugiTransactionException {
        if (nextLowRecord()) {
            var record = getRecord();
            record.reset();
            R result = convertRecord(record);
            event(null, listener -> listener.readRecord(this, result));
            return Optional.of(result);
        } else {
            return Optional.empty();
        }
    }

    protected TsurugiResultRecord getRecord() throws IOException, TsurugiTransactionException {
        if (this.record == null) {
            try {
                var lowResultSet = getLowResultSet();
                this.record = new TsurugiResultRecord(lowResultSet, convertUtil);
            } catch (Throwable e) {
                event(e, listener -> listener.readException(this, e));
                throw e;
            }
        }
        return this.record;
    }

    protected R convertRecord(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
        R result;
        try {
            result = resultMapping.convert(record);
        } catch (Throwable e) {
            event(e, listener -> listener.readException(this, e));
            throw e;
        }
        this.readCount++;
        return result;
    }

    /**
     * get number of read
     *
     * @return number of read
     */
    public int getReadCount() {
        return this.readCount;
    }

    /**
     * get record list
     *
     * @return list of record
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public List<R> getRecordList() throws IOException, TsurugiTransactionException {
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
     * @throws UncheckedIOException
     * @throws TsurugiTransactionRuntimeException
     */
    @Override
    public Iterator<R> iterator() {
        try {
            var record = getRecord();
            return new TsurugiResultSetIterator(record);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (TsurugiTransactionException e) {
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    protected class TsurugiResultSetIterator implements Iterator<R> {
        private final TsurugiResultRecord record;
        private boolean moveNext = true;
        private boolean hasNext;

        public TsurugiResultSetIterator(TsurugiResultRecord record) {
            this.record = record;
        }

        protected void moveNext() {
            if (this.moveNext) {
                try {
                    this.hasNext = nextLowRecord();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } catch (TsurugiTransactionException e) {
                    throw new TsurugiTransactionRuntimeException(e);
                } finally {
                    record.reset();
                }
                this.moveNext = false;
            }
        }

        @Override
        public boolean hasNext() {
            moveNext();
            return this.hasNext;
        }

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
                throw new UncheckedIOException(e);
            } catch (TsurugiTransactionException e) {
                throw new TsurugiTransactionRuntimeException(e);
            }
            event(null, listener -> listener.readRecord(TsurugiResultSet.this, result));
            this.moveNext = true;
            return result;
        }
    }

    @Override
    public void close() throws IOException, TsurugiTransactionException {
        LOG.trace("rs close start");

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

        LOG.trace("rs close end");
    }
}
