package com.tsurugidb.iceaxe.result;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionRuntimeException;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.jogasaki.proto.SqlResponse.ResultOnly;

/**
 * Tsurugi Result Set for PreparedStatement
 * 
 * @param <R> result type
 */
@NotThreadSafe
public class TsurugiResultSet<R> extends TsurugiResult implements Iterable<R> {

    private FutureResponse<ResultSet> lowResultSetFuture;
    private ResultSet lowResultSet;
    private final TgResultMapping<R> resultMapping;
    private final IceaxeConvertUtil convertUtil;
    private final IceaxeTimeout rsTimeout;
    private final IceaxeTimeout closeTimeout;
    private TsurugiResultRecord record;

    // internal
    public TsurugiResultSet(TsurugiTransaction transaction, FutureResponse<ResultSet> lowResultSetFuture, TgResultMapping<R> resultMapping, IceaxeConvertUtil convertUtil) {
        super(transaction);
        this.lowResultSetFuture = lowResultSetFuture;
        this.resultMapping = resultMapping;
        this.convertUtil = convertUtil;
        var info = transaction.getSessionInfo();
        this.rsTimeout = new IceaxeTimeout(info, TgTimeoutKey.RS_CONNECT);
        this.closeTimeout = new IceaxeTimeout(info, TgTimeoutKey.RESULT_CLOSE);

        applyCloseTimeout();
    }

    private void applyCloseTimeout() {
//      closeTimeout.apply(lowResultSet);
        closeTimeout.apply(lowResultSetFuture);
    }

    /**
     * set ResetSet-timeout
     * 
     * @param timeout time
     */
    public void setResultSetTimeout(long time, TimeUnit unit) {
        setResultSetTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * set ResetSet-timeout
     * 
     * @param timeout time
     */
    public void setResultSetTimeout(TgTimeValue timeout) {
        rsTimeout.set(timeout);
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

    protected final synchronized ResultSet getLowResultSet() throws IOException {
        if (this.lowResultSet == null) {
            this.lowResultSet = IceaxeIoUtil.getFromFuture(lowResultSetFuture, rsTimeout);
            try {
                IceaxeIoUtil.close(lowResultSetFuture);
                this.lowResultSetFuture = null;
            } finally {
                applyCloseTimeout();
            }
        }
        return this.lowResultSet;
    }

    @Override
    protected FutureResponse<ResultOnly> getLowResultOnlyFuture() throws IOException {
        return getLowResultSet().getResponse();
    }

    protected boolean nextLowRecord() throws IOException, TsurugiTransactionException {
        try {
            var lowResultSet = getLowResultSet();
            boolean exists = lowResultSet.nextRecord();
            checkResultStatus(false);
            return exists;
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    /**
     * get name list
     * 
     * @return list of column name
     * @throws IOException
     */
    public List<String> getNameList() throws IOException {
        return getNameList(getLowResultSet());
    }

    static List<String> getNameList(ResultSet lowResultSet) throws IOException {
        var lowMeta = lowResultSet.getRecordMeta();
        var size = lowMeta.fieldCount();
        var list = new ArrayList<String>(size);
        for (int i = 0; i < size; i++) {
            var name = lowMeta.name(i);
            list.add(name);
        }
        return list;
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
            var result = resultMapping.convert(record);
            return Optional.of(result);
        } else {
            return Optional.empty();
        }
    }

    protected TsurugiResultRecord getRecord() throws IOException {
        if (this.record == null) {
            var lowResultSet = getLowResultSet();
            this.record = new TsurugiResultRecord(lowResultSet, convertUtil);
        }
        return this.record;
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
            var result = resultMapping.convert(record);
            list.add(result);
        }
        return list;
    }

    /**
     * @throws TsurugiTransactionUncheckedIOException
     */
    @Override
    public Iterator<R> iterator() {
        try {
            var record = getRecord();
            return new TsurugiResultSetIterator(record);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
                result = resultMapping.convert(record);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            this.moveNext = true;
            return result;
        }
    }

    @Override
    public void close() throws IOException {
        // TODO checkResultStatusが廃止されたら、コメントも削除
        // checkResultStatus(true); クローズ時にはステータスチェックは行わない
        // 一度もレコードを取得していない場合でも、commitでステータスチェックされる

        // not try-finally
        IceaxeIoUtil.close(lowResultSet, lowResultSetFuture);
        super.close();
    }
}
