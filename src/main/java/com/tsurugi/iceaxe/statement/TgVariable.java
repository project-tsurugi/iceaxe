package com.tsurugi.iceaxe.statement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import com.nautilus_technologies.tsubakuro.protos.CommonProtos.DataType;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.PlaceHolder;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.PlaceHolder.Variable;

/**
 * Tsurugi Variable
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

    protected static final Map<Object, DataType> TYPE_MAP;
    static {
        Map<Object, DataType> map = new IdentityHashMap<>();
        for (TgDataType type : TgDataType.values()) {
            var lowType = type.getLowDataType();
            map.put(type, lowType);

            for (Class<?> c : type.getClassList()) {
                map.put(c, lowType);
            }
        }
        TYPE_MAP = Collections.unmodifiableMap(map);
    }

    protected static final class TgVariableElement { // record
        private final String name;
        /** low type */
        private final DataType type;

        public TgVariableElement(String name, DataType type) {
            this.name = name;
            this.type = type;
        }

        public String name() {
            return name;
        }

        public DataType type() {
            return type;
        }

        @Override
        public String toString() {
            return name + ":" + type;
        }
    }

    private final List<TgVariableElement> variableList = new ArrayList<>();
    private PlaceHolder lowPlaceHolder;

    /**
     * Tsurugi Variable
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
        add(name, DataType.INT4);
        return this;
    }

    /**
     * add type(long)
     * 
     * @param name name
     * @return this
     */
    public TgVariable int8(String name) {
        add(name, DataType.INT8);
        return this;
    }

    /**
     * add type(float)
     * 
     * @param name name
     * @return this
     */
    public TgVariable float4(String name) {
        add(name, DataType.FLOAT4);
        return this;
    }

    /**
     * add type(double)
     * 
     * @param name name
     * @return this
     */
    public TgVariable float8(String name) {
        add(name, DataType.FLOAT8);
        return this;
    }

    /**
     * add type(String)
     * 
     * @param name name
     * @return this
     */
    public TgVariable character(String name) {
        add(name, DataType.CHARACTER);
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
        var lowType = getLowDataType(type);
        add(name, lowType);
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
        var lowType = getLowDataType(type);
        add(name, lowType);
        return this;
    }

    protected DataType getLowDataType(Object type) {
        var lowType = TYPE_MAP.get(type);
        if (lowType == null) {
            throw new IllegalArgumentException("unsupported data type. type=" + type);
        }
        return lowType;
    }

    protected final void add(String name, DataType lowType) {
        variableList.add(new TgVariableElement(name, lowType));
        this.lowPlaceHolder = null;
    }

    // internal
    public PlaceHolder toLowPlaceHolder() {
        if (this.lowPlaceHolder == null) {
            var lowBuilder = PlaceHolder.newBuilder();
            for (var variable : variableList) {
                var lowVariable = Variable.newBuilder().setName(variable.name()).setType(variable.type()).build();
                lowBuilder.addVariables(lowVariable);
            }
            this.lowPlaceHolder = lowBuilder.build();
        }
        return this.lowPlaceHolder;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + variableList;
    }
}
