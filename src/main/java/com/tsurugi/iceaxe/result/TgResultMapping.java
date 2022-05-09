package com.tsurugi.iceaxe.result;

import java.io.IOException;
import java.util.function.Supplier;

import com.tsurugi.iceaxe.util.IoFunction;

/**
 * Tsurugi Result Mapping
 * 
 * @param <R> result type
 */
public abstract class TgResultMapping<R> {

    /**
     * Result Mapping (convert to {@link TsurugiResultEntity})
     */
    public static final TgResultMapping<TsurugiResultEntity> DEFAULT = new TgResultMapping<>() {

        @Override
        protected TsurugiResultEntity convert(TsurugiResultRecord record) throws IOException {
            return TsurugiResultEntity.of(record);
        }
    };

    /**
     * create Result Mapping
     * 
     * @param <R>             result type
     * @param resultConverter converter from TsurugiResultRecord to R
     * @return Result Mapping
     */
    public static <R> TgResultMapping<R> of(IoFunction<TsurugiResultRecord, R> resultConverter) {
        return TgConverterResultMapping.of(resultConverter);
    }

    /**
     * create Result Mapping
     * 
     * @param <R>            result type
     * @param entitySupplier supplier of R
     * @return Result Mapping
     */
    public static <R> TgEntityResultMapping<R> of(Supplier<R> entitySupplier) {
        return TgEntityResultMapping.of(entitySupplier);
    }

    // internal
    protected abstract R convert(TsurugiResultRecord record) throws IOException;
}
