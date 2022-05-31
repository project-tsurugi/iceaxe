package com.tsurugidb.iceaxe.statement;

import java.util.EnumMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import com.tsurugidb.jogasaki.proto.SqlCommon.AtomType;

/**
 * Tsurugi Data Type
 */
public enum TgDataType {
    // FIXME byte.class等もINT4に含めるか？
    /**
     * int
     */
    INT4(AtomType.INT4, List.of(int.class, Integer.class)),
    /**
     * long
     */
    INT8(AtomType.INT8, List.of(long.class, Long.class)),
    /**
     * float
     */
    FLOAT4(AtomType.FLOAT4, List.of(float.class, Float.class)),
    /**
     * double
     */
    FLOAT8(AtomType.FLOAT8, List.of(double.class, Double.class)),
    /**
     * String
     */
    CHARACTER(AtomType.CHARACTER, List.of(String.class));

    private final AtomType lowType;
    private final List<Class<?>> classList;

    private TgDataType(AtomType lowType, List<Class<?>> classList) {
        this.lowType = lowType;
        this.classList = classList;
    }

    // internal
    public AtomType getLowDataType() {
        return this.lowType;
    }

    protected static final Map<Class<?>, TgDataType> TYPE_MAP;
    static {
        Map<Class<?>, TgDataType> map = new IdentityHashMap<>();
        for (TgDataType type : TgDataType.values()) {
            for (Class<?> c : type.classList) {
                map.put(c, type);
            }
        }
        TYPE_MAP = map;
    }

    /**
     * get data type
     * 
     * @param clazz class
     * @return Tsurugi Data Type
     */
    public static TgDataType of(Class<?> clazz) {
        var type = TYPE_MAP.get(clazz);
        if (type == null) {
            throw new InternalError("unsupported type error. class=" + clazz);
        }
        return type;
    }

    private static final Map<AtomType, TgDataType> LOW_TYPE_MAP;
    static {
        var map = new EnumMap<AtomType, TgDataType>(AtomType.class);
        for (var type : values()) {
            var lowType = type.getLowDataType();
            map.put(lowType, type);
        }
        LOW_TYPE_MAP = map;
    }

    // internal
    public static TgDataType of(AtomType lowType) {
        var type = LOW_TYPE_MAP.get(lowType);
        if (type == null) {
            throw new InternalError("unsupported type error. lowType=" + lowType);
        }
        return type;
    }
}