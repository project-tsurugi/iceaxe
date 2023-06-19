package com.tsurugidb.iceaxe.metadata;

import java.util.List;

import com.tsurugidb.tsubakuro.sql.TableList;

/**
 * Tsurugi table list
 */
public class TgTableList {

    protected final TableList lowTableList;

    /**
     * Creates a new instance.
     *
     * @param lowTableList low TableMetadata
     */
    public TgTableList(TableList lowTableList) {
        this.lowTableList = lowTableList;
    }

    /**
     * Returns a list of the available table names in the database, except system tables.
     *
     * @return a list of the available table names
     */
    public List<String> getTableNameList() {
        return lowTableList.getTableNames();
    }
}
