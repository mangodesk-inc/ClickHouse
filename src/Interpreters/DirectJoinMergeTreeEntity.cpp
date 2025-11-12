#include <Interpreters/DirectJoinMergeTreeEntity.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Set.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageSnapshot.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

DirectJoinMergeTreeEntity::DirectJoinMergeTreeEntity(
        QueryPlan && lookup_plan_,
        const String & primary_key_column_,
        ContextPtr context_)
    : lookup_plan(std::move(lookup_plan_))
    , primary_key_column(primary_key_column_)
    , context(context_)
    , log(getLogger("DirectJoinMergeTreeEntity"))
{
}

Names DirectJoinMergeTreeEntity::getPrimaryKey() const
{
    return {primary_key_column};
}

Block DirectJoinMergeTreeEntity::getSampleBlock(const Names & required_columns) const
{
    const auto & sample_block = lookup_plan.getCurrentHeader();

    if (required_columns.empty())
        return *sample_block;

    Block result_block;
    for (const auto & column_name : required_columns)
        result_block.insert(sample_block->getByName(column_name));

    return result_block;
}

static ActionsDAG buildFilterDAG(const ColumnWithTypeAndName & key_column, const SharedHeader & source_header)
{
    ActionsDAG filter_dag(source_header->getColumnsWithTypeAndName());

    /// Get the primary key column node
    const auto * key_node = filter_dag.tryFindInOutputs(key_column.name);
    if (!key_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Primary key column {} not found in source header {}", key_column.name, source_header->dumpStructure());

    auto set = std::make_shared<Set>(SizeLimits(), /*fill_set_elements=*/ true, /*transform_null_in=*/ false);
    set->setHeader({key_column});
    set->insertFromColumns(Columns{key_column.column});
    set->finishInsert();

    size_t total_elements = set->getTotalRowCount();

    DataTypePtr set_type = std::make_shared<DataTypeSet>();
    auto future_set = std::make_shared<FutureSetFromStorage>(CityHash_v1_0_2::uint128{}, ASTPtr{}, set, std::nullopt);
    auto set_column = ColumnSet::create(total_elements, future_set);
    ColumnWithTypeAndName set_const_column(ColumnConst::create(std::move(set_column), total_elements), set_type, "__set");

    const auto * set_node = &filter_dag.addColumn(std::move(set_const_column));

    auto in_function = FunctionFactory::instance().get("in", nullptr);
    const auto * filter_node = &filter_dag.addFunction(in_function, {key_node, set_node}, {});

    filter_dag.getOutputs().push_back(filter_node);
    return filter_dag;
}

Chunk DirectJoinMergeTreeEntity::executePlan(QueryPlan & plan) const
{
    QueryPlanOptimizationSettings optimization_settings(context);
    BuildQueryPipelineSettings build_settings(context);

    plan.optimize(optimization_settings);
    auto pipeline_builder = plan.buildQueryPipeline(optimization_settings, build_settings);
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder));

    PullingPipelineExecutor executor(pipeline);

    Chunk result_chunk;
    Block result_block;

    while (executor.pull(result_block))
    {
        if (!result_chunk)
        {
            result_chunk = Chunk(result_block.getColumns(), result_block.rows());
        }
        else
        {
            auto columns = result_chunk.detachColumns();
            const auto & new_columns = result_block.getColumns();

            for (size_t i = 0; i < columns.size(); ++i)
            {
                auto mutable_col = IColumn::mutate(std::move(columns[i]));
                mutable_col->insertRangeFrom(*new_columns[i], 0, new_columns[i]->size());
                columns[i] = std::move(mutable_col);
            }

            result_chunk.setColumns(std::move(columns), result_chunk.getNumRows() + result_block.rows());
        }
    }

    if (!result_chunk)
    {
        auto sample_block = getSampleBlock(key_value_result_names);
        result_chunk = Chunk(sample_block.cloneEmptyColumns(), 0);
    }

    return result_chunk;
}

Chunk DirectJoinMergeTreeEntity::getByKeys(
    const ColumnsWithTypeAndName & keys,
    PaddedPODArray<UInt8> & out_null_map,
    const Names & required_columns) const
{
    if (keys.empty())
        return Chunk();

    if (keys.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DirectJoinMergeTreeEntity only supports single-column keys");
    if (keys[0].column->empty())
    {
        auto sample_block = getSampleBlock(required_columns);
        return Chunk(sample_block.cloneEmptyColumns(), 0);
    }

    if (!lookup_plan.isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query plan for direct join is not initialized");

    const size_t num_keys = keys[0].column->size();
    LOG_TRACE(log, "XXXX Looking up {} keys", num_keys);

    QueryPlan plan = lookup_plan.clone();
    // plan.resolveStorages(context);

    {
        auto filter_dag = buildFilterDAG(keys[0], plan.getCurrentHeader());
        auto filter_step = std::make_unique<FilterStep>(
            plan.getCurrentHeader(),
            std::move(filter_dag),
            filter_dag.getOutputs().back()->result_name,
            /*remove_filter_column=*/ true);
        filter_step->setStepDescription("Filter by join keys");
        plan.addStep(std::move(filter_step));
    }

    auto result_chunk = executePlan(plan);

    UNUSED(out_null_map);
    UNUSED(required_columns);
    return result_chunk;
}

}
