#pragma once

#include <Common/PODArray_fwd.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Names.h>
#include <Processors/Chunk.h>
#include <Interpreters/IKeyValueEntity.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageSnapshot.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

class IStorage;

class DirectJoinMergeTreeEntity : public IKeyValueEntity
{
public:
    DirectJoinMergeTreeEntity(QueryPlan && lookup_plan_, const String & primary_key_column_, ContextPtr context_);

    Names getPrimaryKey() const override;

    Chunk getByKeys(
        const ColumnsWithTypeAndName & keys,
        PaddedPODArray<UInt8> & out_null_map,
        const Names & required_columns) const override;

    Block getSampleBlock(const Names & required_columns) const override;

private:
    Chunk executePlan(QueryPlan & plan) const;

    QueryPlan lookup_plan;
    String primary_key_column;
    ContextPtr context;

    LoggerPtr log;
};

}
