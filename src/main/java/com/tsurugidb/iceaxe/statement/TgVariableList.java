package com.tsurugidb.iceaxe.statement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tsurugidb.jogasaki.proto.SqlRequest.PlaceHolder;

/**
 * Tsurugi Variable definition for PreparedStatement
 */
public class TgVariableList {

    /**
     * create Tsurugi Variable
     * 
     * @return Tsurugi Variable
     */
    public static TgVariableList of() {
        return new TgVariableList();
    }

    /**
     * create Tsurugi Variable
     * 
     * @param variables variable
     * @return Tsurugi Variable
     */
    public static TgVariableList of(TgVariable<?>... variables) {
        var variableList = new TgVariableList();
        for (var variable : variables) {
            variableList.add(variable);
        }
        return variableList;
    }

    private final List<PlaceHolder> lowPlaceHolderList = new ArrayList<>();
    /** Map&lt;name, type&gt; */
    private Map<String, TgDataType> typeMap;

    /**
     * Tsurugi Variable definition for PreparedStatement
     */
    public TgVariableList() {
        // do nothing
    }

    /**
     * add type(int)
     * 
     * @param name name
     * @return this
     */
    public TgVariableList int4(String name) {
        addInternal(name, TgDataType.INT4);
        return this;
    }

    /**
     * add type(long)
     * 
     * @param name name
     * @return this
     */
    public TgVariableList int8(String name) {
        addInternal(name, TgDataType.INT8);
        return this;
    }

    /**
     * add type(float)
     * 
     * @param name name
     * @return this
     */
    public TgVariableList float4(String name) {
        addInternal(name, TgDataType.FLOAT4);
        return this;
    }

    /**
     * add type(double)
     * 
     * @param name name
     * @return this
     */
    public TgVariableList float8(String name) {
        addInternal(name, TgDataType.FLOAT8);
        return this;
    }

    /**
     * add type(String)
     * 
     * @param name name
     * @return this
     */
    public TgVariableList character(String name) {
        addInternal(name, TgDataType.CHARACTER);
        return this;
    }

    /**
     * add type
     * 
     * @param name name
     * @param type data type
     * @return this
     */
    public TgVariableList add(String name, TgDataType type) {
        addInternal(name, type);
        return this;
    }

    /**
     * add type
     * 
     * @param name name
     * @param type data type
     * @return this
     */
    public TgVariableList add(String name, Class<?> type) {
        var tgType = getDataType(type);
        addInternal(name, tgType);
        return this;
    }

    protected TgDataType getDataType(Class<?> type) {
        var tgType = TgDataType.of(type);
        if (tgType == null) {
            throw new IllegalArgumentException("unsupported data type. type=" + type);
        }
        return tgType;
    }

    /**
     * add variable
     * 
     * @param variable variable
     * @return this
     */
    public TgVariableList add(TgVariable<?> variable) {
        var name = variable.name();
        var type = variable.type();
        addInternal(name, type);
        return this;
    }

    protected final void addInternal(String name, TgDataType type) {
        var lowVariable = PlaceHolder.newBuilder().setName(name).setType(type.getLowDataType()).build();
        lowPlaceHolderList.add(lowVariable);
        this.typeMap = null;
    }

    // internal
    public List<PlaceHolder> toLowPlaceHolderList() {
        return this.lowPlaceHolderList;
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
                for (var v : lowPlaceHolderList) {
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
        return getClass().getSimpleName() + lowPlaceHolderList;
    }
}
