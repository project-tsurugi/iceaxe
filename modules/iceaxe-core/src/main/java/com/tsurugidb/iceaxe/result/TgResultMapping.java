package com.tsurugidb.iceaxe.result;

import java.io.IOException;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.function.TsurugiTransactionFunction;

/**
 * Tsurugi Result Mapping
 * 
 * @param <R> result type
 */
@ThreadSafe
public abstract class TgResultMapping<R> {

    /**
     * Result Mapping (convert to {@link TsurugiResultEntity})
     */
    public static final TgResultMapping<TsurugiResultEntity> DEFAULT = new TgResultMapping<>() {

        @Override
        protected TsurugiResultEntity convert(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
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
    public static <R> TgResultMapping<R> of(TsurugiTransactionFunction<TsurugiResultRecord, R> resultConverter) {
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

    private IceaxeConvertUtil convertUtil = null;

    /**
     * set convert type utility
     * 
     * @param convertUtil convert type utility
     */
    public void setConvertUtil(IceaxeConvertUtil convertUtil) {
        this.convertUtil = convertUtil;
    }

    // internal
    public IceaxeConvertUtil getConvertUtil() {
        return this.convertUtil;
    }

    // internal
    protected abstract R convert(TsurugiResultRecord record) throws IOException, TsurugiTransactionException;
}
