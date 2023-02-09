package com.tsurugidb.iceaxe.explain;

import java.util.List;

import com.tsurugidb.sql.proto.SqlCommon.Column;
import com.tsurugidb.tsubakuro.explain.PlanGraph;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.explain.PlanGraphLoader;
import com.tsurugidb.tsubakuro.explain.json.JsonPlanGraphLoader;
import com.tsurugidb.tsubakuro.sql.StatementMetadata;

/**
 * Tsurugi statement metadata
 */
public class TgStatementMetadata {

    protected final String source;
    protected final StatementMetadata lowStatementMetadata;

    /**
     * Creates a new instance.
     *
     * @param source               SQL statement
     * @param lowStatementMetadata low StatementMetadata
     */
    public TgStatementMetadata(String source, StatementMetadata lowStatementMetadata) {
        this.source = source;
        this.lowStatementMetadata = lowStatementMetadata;
    }

    /**
     * get plan graph.
     *
     * @return plan graph
     * @throws PlanGraphException if the plan text is not valid to extract plan graph
     */
    public PlanGraph getLowPlanGraph() throws PlanGraphException {
        var loader = createPlanGraphLoader();
        var formatId = lowStatementMetadata.getFormatId();
        var formatVersion = lowStatementMetadata.getFormatVersion();
        var contents = lowStatementMetadata.getContents();
        return loader.load(formatId, formatVersion, contents);
    }

    protected PlanGraphLoader createPlanGraphLoader() {
        return JsonPlanGraphLoader.newBuilder().build();
    }

    /**
     * get metadata contents.
     *
     * @return contents
     */
    public String getMetadataContents() {
        return lowStatementMetadata.getContents();
    }

    /**
     * get column list.
     *
     * @return column list
     */
    public List<? extends Column> getLowColumnList() {
        return lowStatementMetadata.getColumns();
    }

    @Override
    public String toString() {
        return "explain[" + source + "]";
    }
}
