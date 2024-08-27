/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe.metadata;

import java.util.List;

import com.tsurugidb.tsubakuro.sql.TableList;

/**
 * Tsurugi table list.
 */
public class TgTableList {

    /** low table list */
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
