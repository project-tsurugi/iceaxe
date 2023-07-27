package com.tsurugidb.iceaxe.sql.result;

import java.io.IOException;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.result.mapping.TgConverterResultMapping;
import com.tsurugidb.iceaxe.sql.result.mapping.TgEntityResultMapping;
import com.tsurugidb.iceaxe.sql.result.mapping.TgSingleResultMapping;
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
        protected TsurugiResultEntity convert(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
            return TsurugiResultEntity.of(record);
        }
    };

    /**
     * create result mapping
     *
     * @param <R>             result type
     * @param resultConverter converter from TsurugiResultRecord to R
     * @return result mapping
     */
    public static <R> TgResultMapping<R> of(TsurugiTransactionFunction<TsurugiResultRecord, R> resultConverter) {
        return TgConverterResultMapping.of(resultConverter);
    }

    /**
     * create result mapping
     *
     * @param <R>            result type
     * @param entitySupplier supplier of R
     * @return result mapping
     */
    public static <R> TgEntityResultMapping<R> of(Supplier<R> entitySupplier) {
        return TgEntityResultMapping.of(entitySupplier);
    }

    /**
     * create result mapping (single column)
     *
     * @param <R>   result type
     * @param clazz result type
     * @return result mapping
     */
    public static <R> TgResultMapping<R> ofSingle(Class<R> clazz) {
        return TgSingleResultMapping.of(clazz);
    }

    /**
     * create result mapping (single column)
     *
     * @param <R>  result type
     * @param type result type
     * @return result mapping
     */
    public static <R> TgResultMapping<R> ofSingle(TgDataType type) {
        return TgSingleResultMapping.of(type);
    }

    private IceaxeConvertUtil convertUtil = null;

    /**
     * set convert type utility
     *
     * @param convertUtil convert type utility
     * @return this
     */
    public TgResultMapping<R> setConvertUtil(IceaxeConvertUtil convertUtil) {
        this.convertUtil = convertUtil;
        return this;
    }

    /**
     * get convert type utility
     *
     * @return convert type utility
     */
    public IceaxeConvertUtil getConvertUtil() {
        return this.convertUtil;
    }

    protected abstract R convert(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException;
}
