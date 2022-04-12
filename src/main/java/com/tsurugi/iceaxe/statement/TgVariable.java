package com.tsurugi.iceaxe.statement;

import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import com.nautilus_technologies.tsubakuro.protos.RequestProtos.PlaceHolder;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.PlaceHolder.Variable;

/**
 * Tsurugi Variable definition for PreparedStatement
 */
public class TgVariable {

    /**
     * create Tsurugi Variable
     * 
     * @return Tsurugi Variable
     */
    public static TgVariable of() {
        return new TgVariable();
    }

    protected static final Map<Object, TgDataType> TYPE_MAP;
    static {
        Map<Object, TgDataType> map = new IdentityHashMap<>();
        for (TgDataType type : TgDataType.values()) {
            for (Class<?> c : type.getClassList()) {
                map.put(c, type);
            }
        }
        TYPE_MAP = Collections.unmodifiableMap(map);
    }

    private final PlaceHolder.Builder lowBuilder = PlaceHolder.newBuilder();
    private PlaceHolder lowPlaceHolder;
    /** Map&lt;name, type&gt; */
    private Map<String, TgDataType> typeMap;

    /**
     * Tsurugi Variable definition for PreparedStatement
     */
    public TgVariable() {
        // do nothing
    }

    // FIXME nameを可変長引数にした方が便利か？（そんな使い方はしないか？）
    /**
     * add type(int)
     * 
     * @param name name
     * @return this
     */
    public TgVariable int4(String name) {
        add(name, TgDataType.INT4);
        return this;
    }

    /**
     * add type(long)
     * 
     * @param name name
     * @return this
     */
    public TgVariable int8(String name) {
        add(name, TgDataType.INT8);
        return this;
    }

    /**
     * add type(float)
     * 
     * @param name name
     * @return this
     */
    public TgVariable float4(String name) {
        add(name, TgDataType.FLOAT4);
        return this;
    }

    /**
     * add type(double)
     * 
     * @param name name
     * @return this
     */
    public TgVariable float8(String name) {
        add(name, TgDataType.FLOAT8);
        return this;
    }

    /**
     * add type(String)
     * 
     * @param name name
     * @return this
     */
    public TgVariable character(String name) {
        add(name, TgDataType.CHARACTER);
        return this;
    }

    // FIXME setメというソッド名よりaddの方がいいか？
    /**
     * add type
     * 
     * @param name name
     * @param type data type
     * @return this
     */
    public TgVariable set(String name, TgDataType type) {
        add(name, type);
        return this;
    }

    /**
     * add type
     * 
     * @param name name
     * @param type data type
     * @return this
     */
    public TgVariable set(String name, Class<?> type) {
        var lowType = getDataType(type);
        add(name, lowType);
        return this;
    }

    protected TgDataType getDataType(Object type) {
        var tgType = TYPE_MAP.get(type);
        if (tgType == null) {
            throw new IllegalArgumentException("unsupported data type. type=" + type);
        }
        return tgType;
    }

    protected final void add(String name, TgDataType type) {
        var lowVariable = Variable.newBuilder().setName(name).setType(type.getLowDataType()).build();
        lowBuilder.addVariables(lowVariable);
        this.lowPlaceHolder = null;
        this.typeMap = null;
    }

    // internal
    public synchronized PlaceHolder toLowPlaceHolder() {
        if (this.lowPlaceHolder == null) {
            this.lowPlaceHolder = lowBuilder.build();
        }
        return this.lowPlaceHolder;
    }

    /**
     * get data type
     * 
     * @param name name
     * @return data type
     */
    public TgDataType getDataType(String name) {
        if (this.typeMap == null) {
            synchronized (this) {
                var map = new HashMap<String, TgDataType>();
                var list = lowBuilder.getVariablesList();
                for (var v : list) {
                    var lowName = v.getName();
                    var lowType = v.getType();
                    var type = TgDataType.of(lowType);
                    map.put(lowName, type);
                }
                this.typeMap = map;
            }
        }

        var type = typeMap.get(name);
        if (type == null) {
            throw new IllegalArgumentException("not found type. name=" + name);
        }
        return type;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + lowBuilder + "]";
    }
}
