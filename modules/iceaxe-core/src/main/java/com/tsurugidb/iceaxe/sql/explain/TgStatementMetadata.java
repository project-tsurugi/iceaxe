package com.tsurugidb.iceaxe.sql.explain;

import java.util.List;

import javax.annotation.Nullable;

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
    protected final Object arguments;
    protected final StatementMetadata lowStatementMetadata;

    /**
     * Creates a new instance.
     *
     * @param source               SQL statement
     * @param arguments            SQL arguments
     * @param lowStatementMetadata low StatementMetadata
     */
    public TgStatementMetadata(String source, @Nullable Object arguments, StatementMetadata lowStatementMetadata) {
        this.source = source;
        this.arguments = arguments;
        this.lowStatementMetadata = lowStatementMetadata;
    }

    /**
     * get source.
     *
     * @return source
     */
    public String getSource() {
        return this.source;
    }

    /**
     * get arguments.
     *
     * @return SQL arguments
     */
    public <P> @Nullable P getArguments() {
        @SuppressWarnings("unchecked")
        var r = (P) this.arguments;
        return r;
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
        if (this.arguments == null) {
            return "explain[" + source + "]";
        }
        return "explain[" + source + "][" + arguments + "]";
    }
}
