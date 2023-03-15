package com.tsurugidb.iceaxe.sql.result.mapping;

import java.io.IOException;

import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.function.TsurugiTransactionFunction;

/**
 * Tsurugi Result Mapping
 * 
 * @param <R> result type
 */
public class TgConverterResultMapping<R> extends TgResultMapping<R> {

    /**
     * create Result Mapping
     * 
     * @param <R>             result type
     * @param resultConverter converter from TsurugiResultRecord to R
     * @return Result Mapping
     */
    public static <R> TgConverterResultMapping<R> of(TsurugiTransactionFunction<TsurugiResultRecord, R> resultConverter) {
        return new TgConverterResultMapping<>(resultConverter);
    }

    private final TsurugiTransactionFunction<TsurugiResultRecord, R> resultConverter;

    /**
     * Tsurugi Result Mapping
     * 
     * @param resultConverter converter from TsurugiResultRecord to R
     */
    public TgConverterResultMapping(TsurugiTransactionFunction<TsurugiResultRecord, R> resultConverter) {
        this.resultConverter = resultConverter;
    }

    @Override
    protected R convert(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
        return resultConverter.apply(record);
    }
}
