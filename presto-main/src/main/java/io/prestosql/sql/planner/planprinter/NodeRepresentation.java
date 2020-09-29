/*
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
package io.prestosql.sql.planner.planprinter;

import io.prestosql.cost.PlanCostEstimate;
import io.prestosql.cost.PlanNodeStatsAndCostSummary;
import io.prestosql.cost.PlanNodeStatsEstimate;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NodeRepresentation
{
    private final PlanNodeId id;
    private final String name;
    private final String type;
    private final String identifier;
    private final List<TypedSymbol> outputs;
    private final List<PlanNodeId> children;
    private final List<PlanFragmentId> remoteSources;
    private final Optional<PlanNodeStats> stats;
    private final List<PlanNodeStatsEstimate> estimatedStats;
    private final List<PlanCostEstimate> estimatedCost;
    private final Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost;

    private final StringBuilder details = new StringBuilder();

    public NodeRepresentation(
            PlanNodeId id,
            String name,
            String type,
            String identifier,
            List<TypedSymbol> outputs,
            Optional<PlanNodeStats> stats,
            List<PlanNodeStatsEstimate> estimatedStats,
            List<PlanCostEstimate> estimatedCost,
            Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost,
            List<PlanNodeId> children,
            List<PlanFragmentId> remoteSources)
    {
        this.id = requireNonNull(id, "id is null");
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.identifier = requireNonNull(identifier, "identifier is null");
        this.outputs = requireNonNull(outputs, "outputs is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.estimatedStats = requireNonNull(estimatedStats, "estimatedStats is null");
        this.estimatedCost = requireNonNull(estimatedCost, "estimatedCost is null");
        this.reorderJoinStatsAndCost = requireNonNull(reorderJoinStatsAndCost, "reorderJoinStatsAndCost is null");
        this.children = requireNonNull(children, "children is null");
        this.remoteSources = requireNonNull(remoteSources, "remoteSources is null");

        checkArgument(estimatedCost.size() == estimatedStats.size(), "size of cost and stats list does not match");
    }

    public void appendDetails(String string, Object... args)
    {
        if (args.length == 0) {
            details.append(string);
        }
        else {
            details.append(format(string, args));
        }
    }

    public void appendDetailsLine(String string, Object... args)
    {
        appendDetails(string, args);
        details.append('\n');
    }

    public PlanNodeId getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }

    public String getType()
    {
        return type;
    }

    public String getIdentifier()
    {
        return identifier;
    }

    public List<TypedSymbol> getOutputs()
    {
        return outputs;
    }

    public List<PlanNodeId> getChildren()
    {
        return children;
    }

    public List<PlanFragmentId> getRemoteSources()
    {
        return remoteSources;
    }

    public String getDetails()
    {
        return details.toString();
    }

    /**
     * Gets the statistics when processing a node in query plan.
     * <p>
     * The statistics are about produced and consumed data and the cpu
     * consumption.
     *
     * @return the statistics when processing a node in query plan
     */
    public Optional<PlanNodeStats> getStats()
    {
        return stats;
    }

    public List<PlanNodeStatsEstimate> getEstimatedStats()
    {
        return estimatedStats;
    }

    /**
     * Gets the list of all estimated cost for the node's children
     *
     * @return all estimated cost for all node's children
     */
    public List<PlanCostEstimate> getEstimatedCost()
    {
        return estimatedCost;
    }

    public Optional<PlanNodeStatsAndCostSummary> getReorderJoinStatsAndCost()
    {
        return reorderJoinStatsAndCost;
    }

    public static class TypedSymbol
    {
        private final Symbol symbol;
        private final String type;

        public TypedSymbol(Symbol symbol, String type)
        {
            this.symbol = symbol;
            this.type = type;
        }

        public Symbol getSymbol()
        {
            return symbol;
        }

        public String getType()
        {
            return type;
        }
    }
}
