package com.tsurugidb.iceaxe.sql.parameter.mapping;

import java.util.List;

import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;;

/**
 * Tsurugi Parameter Mapping for Empty
 *
 * @param <P> parameter type
 */
public class TgEmptyParameterMapping<P> extends TgParameterMapping<P> {

    private static final TgEmptyParameterMapping<?> INSTANCE = new TgEmptyParameterMapping<>();

    /**
     * create Parameter Mapping
     *
     * @param <P> parameter type
     * @return Tsurugi Parameter Mapping
     */
    public static <P> TgEmptyParameterMapping<P> of() {
        @SuppressWarnings("unchecked")
        var r = (TgEmptyParameterMapping<P>) INSTANCE;
        return r;
    }

    @Override
    public List<Placeholder> toLowPlaceholderList() {
        return List.of();
    }

    @Override
    public List<Parameter> toLowParameterList(P parameter, IceaxeConvertUtil convertUtil) {
        return List.of();
    }
}
