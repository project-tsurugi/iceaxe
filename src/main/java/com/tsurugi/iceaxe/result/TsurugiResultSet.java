package com.tsurugi.iceaxe.result;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;
import java.util.function.Function;

import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos.ResultOnly;
import com.tsurugi.iceaxe.statement.TsurugiPreparedStatement;
import com.tsurugi.iceaxe.util.IceaxeIoUtil;

/**
 * Tsurugi Result Set for PreparedStatement
 * 
 * @param <R> record type
 */
public class TsurugiResultSet<R> extends TsurugiResult implements Iterable<R> {

    private Future<ResultSet> lowResultSetFuture;
    private ResultSet lowResultSet;
    private final Function<TsurugiResultRecord, R> recordConverter;

    // internal
    public TsurugiResultSet(TsurugiPreparedStatement preparedStatement, Future<ResultSet> lowResultSetFuture, Future<ResultOnly> lowResultOnlyFuture,
            Function<TsurugiResultRecord, R> recordConverter) {
        super(preparedStatement, lowResultOnlyFuture);
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

    /**
     * @throws UncheckedIOException
     */
    @Override
    public Iterator<R> iterator() {
        try {
            /* var lowRs = */ getLowResultSet();
            return new TsurugiResultSetIterator(/* lowRs */);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected class TsurugiResultSetIterator implements Iterator<R> {
        private boolean moveNext = true;
        private boolean hasNext;

        protected void moveNext() {
            if (this.moveNext) {
                try {
                    this.hasNext = lowResultSet.nextRecord();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
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
            try {
                var record = new TsurugiResultRecord();
                while (lowResultSet.nextColumn()) {
                    var name = "TODO"; // TODO name
                    var value = getLowCurrentColumnValue();
                    record.add(name, value);
                }
                var r = recordConverter.apply(record);
                this.moveNext = true;
                return r;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    protected Object getLowCurrentColumnValue() throws IOException {
        if (lowResultSet.isNull()) {
            return null;
        }
        var lowType = lowResultSet.type();
        switch (lowType) {
        case INT4:
            return lowResultSet.getInt4();
        case INT8:
            return lowResultSet.getInt8();
        case FLOAT4:
            return lowResultSet.getFloat4();
        case FLOAT8:
            return lowResultSet.getFloat8();
        case CHARACTER:
            return lowResultSet.getCharacter();
        default:
            throw new UnsupportedOperationException("unsupported type error. lowType=" + lowType);
        }
    }

    @Override
    public void close() throws IOException {
        // not try-finally
        getLowResultSet().close();
        super.close();
    }
}
