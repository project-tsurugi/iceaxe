package com.tsurugi.iceaxe.result;

import java.io.IOException;

import com.tsurugi.iceaxe.util.IoFunction;

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
    public static <R> TgConverterResultMapping<R> of(IoFunction<TsurugiResultRecord, R> resultConverter) {
        return new TgConverterResultMapping<>(resultConverter);
    }

    private final IoFunction<TsurugiResultRecord, R> resultConverter;

    /**
     * Tsurugi Result Mapping
     * 
     * @param resultConverter converter from TsurugiResultRecord to R
     */
    public TgConverterResultMapping(IoFunction<TsurugiResultRecord, R> resultConverter) {
        this.resultConverter = resultConverter;
    }

    @Override
    protected R convert(TsurugiResultRecord record) throws IOException {
        return resultConverter.apply(record);
    }
}
