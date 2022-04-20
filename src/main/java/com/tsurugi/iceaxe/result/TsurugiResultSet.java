package com.tsurugi.iceaxe.result;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos.ResultOnly;
import com.tsurugi.iceaxe.transaction.TsurugiTransaction;
import com.tsurugi.iceaxe.transaction.TsurugiTransactionIOException;
import com.tsurugi.iceaxe.transaction.TsurugiTransactionUncheckedIOException;
import com.tsurugi.iceaxe.util.IceaxeIoUtil;
import com.tsurugi.iceaxe.util.IoFunction;

/**
 * Tsurugi Result Set for PreparedStatement
 * <p>
 * MT unsafe
 * </p>
 * 
 * @param <R> record type
 */
public class TsurugiResultSet<R> extends TsurugiResult implements Iterable<R> {

    private Future<ResultSet> lowResultSetFuture;
    private ResultSet lowResultSet;
    private final IoFunction<TsurugiResultRecord, R> recordConverter;
    private TsurugiResultRecord record;

    // internal
    public TsurugiResultSet(TsurugiTransaction transaction, Future<ResultSet> lowResultSetFuture, IoFunction<TsurugiResultRecord, R> recordConverter) {
        super(transaction);
        this.lowResultSetFuture = lowResultSetFuture;
        this.recordConverter = recordConverter;
    }

    protected synchronized final ResultSet getLowResultSet() throws IOException {
        if (this.lowResultSet == null) {
            var info = getSessionInfo();
            this.lowResultSet = IceaxeIoUtil.getFromFuture(lowResultSetFuture, info);
            this.lowResultSetFuture = null;
        }
        return this.lowResultSet;
    }

    @Override
    protected Future<ResultOnly> getLowResultOnlyFuture() throws IOException {
        return getLowResultSet().getResponse();
    }

    protected boolean nextLowRecord() throws IOException, TsurugiTransactionIOException {
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
     * @throws TsurugiTransactionIOException
     */
    public Optional<R> findRecord() throws TsurugiTransactionIOException {
        try {
            if (nextLowRecord()) {
                var record = getRecord();
                record.reset();
                var r = recordConverter.apply(record);
                return Optional.of(r);
            } else {
                return Optional.empty();
            }
        } catch (TsurugiTransactionIOException e) {
            throw e;
        } catch (IOException e) {
            throw new TsurugiTransactionIOException(e);
        }
    }

    protected TsurugiResultRecord getRecord() throws IOException {
        if (this.record == null) {
            var lowResultSet = getLowResultSet();
            this.record = new TsurugiResultRecord(lowResultSet);
        }
        return this.record;
    }

    /**
     * get record list
     * 
     * @return list of record
     * @throws TsurugiTransactionIOException
     */
    public List<R> getRecordList() throws TsurugiTransactionIOException {
        var list = new ArrayList<R>();
        try {
            var record = getRecord();
            while (nextLowRecord()) {
                record.reset();
                var r = recordConverter.apply(record);
                list.add(r);
            }
        } catch (TsurugiTransactionIOException e) {
            throw e;
        } catch (IOException e) {
            throw new TsurugiTransactionIOException(e);
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
            throw new TsurugiTransactionUncheckedIOException(new TsurugiTransactionIOException(e));
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
                } catch (TsurugiTransactionIOException e) {
                    throw new TsurugiTransactionUncheckedIOException(e);
                } catch (IOException e) {
                    throw new TsurugiTransactionUncheckedIOException(new TsurugiTransactionIOException(e));
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
            R r;
            try {
                r = recordConverter.apply(record);
            } catch (TsurugiTransactionIOException e) {
                throw new TsurugiTransactionUncheckedIOException(e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            this.moveNext = true;
            return r;
        }
    }

    @Override
    public void close() throws IOException {
        // checkResultStatus(true); クローズ時にはステータスチェックは行わない
        // 一度もレコードを取得していない場合でも、commitでステータスチェックされる

        // not try-finally
        getLowResultSet().close();
        super.close();
    }
}
