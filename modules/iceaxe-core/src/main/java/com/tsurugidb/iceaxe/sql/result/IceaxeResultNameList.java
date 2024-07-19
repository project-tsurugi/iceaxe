package com.tsurugidb.iceaxe.sql.result;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.sql.proto.SqlCommon.Column;

/**
 * name utility.
 *
 * @since 1.5.0
 */
@IceaxeInternal
@ThreadSafe
public class IceaxeResultNameList {

    /**
     * ambiguous name policy.
     *
     * @since 1.5.0
     */
    public enum IceaxeAmbiguousNamePolicy {
        /** error. */
        ERROR,
        /** get first column. */
        FIRST,
        /** get last column. */
        LAST,
    }

    /**
     * create name utility.
     *
     * @param lowColumnList low column list
     * @return name utility
     */
    public static IceaxeResultNameList of(List<? extends Column> lowColumnList) {
        var nameList = toNameList(lowColumnList);
        return new IceaxeResultNameList(nameList);
    }

    /**
     * convert to name list.
     *
     * @param lowColumnList low column list
     * @return name list
     */
    public static List<String> toNameList(List<? extends Column> lowColumnList) {
        var nameList = new ArrayList<String>(lowColumnList.size());
        int i = 0;
        for (var lowColumn : lowColumnList) {
            var name = getColumnName(lowColumn, i++);
            nameList.add(name);
        }
        return nameList;
    }

    private static String getColumnName(Column lowColumn, int index) {
        var lowName = lowColumn.getName();
        if (lowName == null || lowName.isEmpty()) {
            return "@#" + index;
        }
        return lowName;
    }

    private final List<String> nameList;

    private Map<String, List<Integer>> nameIndexMap = null;

    /**
     * Creates a new instance.
     *
     * @param nameList name list
     */
    public IceaxeResultNameList(List<String> nameList) {
        this.nameList = List.copyOf(nameList);
    }

    /**
     * get name list.
     *
     * @return name list
     */
    public List<String> getNameList() {
        return this.nameList;
    }

    /**
     * get size.
     *
     * @return size of name list
     */
    public int size() {
        return nameList.size();
    }

    /**
     * get name.
     *
     * @param index column index
     * @return column name
     */
    public String getName(int index) {
        return nameList.get(index);
    }

    /**
     * get index.
     *
     * @param name   column name
     * @param policy ambiguous name policy
     * @return column index
     */
    public int getIndex(String name, IceaxeAmbiguousNamePolicy policy) {
        var indexMap = getNameIndexMap();
        List<Integer> indexList = indexMap.get(name);
        if (indexList == null) {
            throw new IllegalArgumentException(MessageFormat.format("not found column. name={0}", name));
        }

        if (indexList.size() == 1) {
            return indexList.get(0);
        }

        switch (policy) {
        case ERROR:
            throw new IllegalArgumentException(MessageFormat.format("column is ambiguous. name={0}", name));
        case FIRST:
            return indexList.get(0); // getHead()
        case LAST:
            return indexList.get(indexList.size() - 1); // getLast()
        default:
            throw new UnsupportedOperationException(MessageFormat.format("unsupported policy. policy={0}", policy));
        }
    }

    /**
     * get index.
     *
     * @param name     column name
     * @param subIndex index for same name
     * @return column index
     */
    public int getIndex(String name, int subIndex) {
        var indexMap = getNameIndexMap();
        List<Integer> indexList = indexMap.get(name);
        if (indexList == null) {
            throw new IllegalArgumentException(MessageFormat.format("not found column. name={0}", name));
        }

        if (0 <= subIndex && subIndex < indexList.size()) {
            return indexList.get(subIndex);
        }

        throw new IllegalArgumentException(MessageFormat.format("not found column. name={0}, subIndex={1}", name, subIndex));
    }

    /**
     * get name-index map.
     *
     * @return name-index map
     */
    protected Map<String, List<Integer>> getNameIndexMap() {
        if (this.nameIndexMap == null) {
            var map = new HashMap<String, List<Integer>>(nameList.size());
            for (int i = 0; i < nameList.size(); i++) {
                String name = nameList.get(i);
                List<Integer> indexList = map.computeIfAbsent(name, k -> new ArrayList<>(2));
                indexList.add(i);
            }
            this.nameIndexMap = map;
        }
        return this.nameIndexMap;
    }

    @Override
    public String toString() {
        var map = this.nameIndexMap;
        if (map != null) {
            return getClass().getSimpleName() + map;
        }

        return getClass().getSimpleName() + nameList;
    }
}
