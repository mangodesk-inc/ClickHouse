#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <Interpreters/InsertDeduplication.h>
#include <Interpreters/InsertDependenciesBuilder.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Chunk.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <Core/Block.h>
#include <Common/PODArray.h>
#include <Common/ErrorCodes.h>
#include <Common/SipHash.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>

#include <base/defines.h>
#include <base/scope_guard.h>

#include <fmt/format.h>
#include <fmt/ranges.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace Setting
{
    extern const SettingsBool use_async_executor_for_materialized_views;
}

static std::atomic<size_t> deduplication_info_id_counter{0};

DeduplicationInfo::FilterResult DeduplicationInfo::filterSelfDuplicate()
{
    LOG_DEBUG(logger, "filterSelfDeduplicate, debug: {}", debug());
    // do not run deduplication for sync insert without async deduplication
    // for sync insert deduplication we need a has from part writer which is set later
    // for async insert we either use usert token or calculate data here
    // updateAnnotation will calculate missing tokens
    if (getCount() <= 1)
        return {original_block, shared_from_this(), 0};

    std::string partition_id = "doesnt_matter_for_self_duplicate";
    auto block_id_to_offsets = buildBlockIdToOffsetsMap(partition_id);

    std::set<size_t> fitered_offsets;
    for (auto & [_, block_offsets] : block_id_to_offsets)
    {
        if (block_offsets.size() > 1)
            fitered_offsets.insert(block_offsets.begin() + 1, block_offsets.end());
    }

    if (fitered_offsets.empty())
    {
        LOG_DEBUG(logger, "No duplicates found in self deduplication, debug: {}", debug());
        return {original_block, shared_from_this(), 0};
    }

    LOG_ERROR(
        logger,
        "Detected duplicate tokens {}, fitered tokens: [{}], debug: {}",
        fitered_offsets.size(),
        fmt::join(fitered_offsets.begin(), fitered_offsets.end(), ","),
        debug());

    return filterImpl(fitered_offsets);
}


DeduplicationInfo::FilterResult DeduplicationInfo::filterOriginalBlock(const std::vector<std::string> & collisions, const String & partition_id)
{
    chassert(original_block && !original_block->empty());

    if (collisions.empty())
        return {.filtered_block = original_block, .deduplication_info = shared_from_this(), .removed_count = 0};

    auto block_id_to_offsets = buildBlockIdToOffsetsMap(partition_id);

    std::set<size_t> fitered_offsets;
    for (const auto & collision :collisions)
        if (auto it = block_id_to_offsets.find(collision); it != block_id_to_offsets.end())
            fitered_offsets.insert(it->second.begin(), it->second.end());

    return filterImpl(fitered_offsets);
}


DeduplicationInfo::Ptr DeduplicationInfo::cloneSelfFilterImpl() const
{
    auto new_instance = DeduplicationInfo::create(is_async_insert);
    new_instance->level = level;
    new_instance->visited_views = visited_views;
    new_instance->last_partition_choice = last_partition_choice;
    new_instance->insert_dependencies = insert_dependencies;
    return new_instance;
}

DeduplicationInfo::FilterResult DeduplicationInfo::filterImpl(const std::set<size_t> & collision_offsets) const
{
    chassert(!collision_offsets.empty());
    chassert(original_block && !original_block->empty());

    auto & block = *original_block;

    Ptr new_tokens = cloneSelfFilterImpl();

    size_t remove_count = 0;
    PaddedPODArray<UInt8> filer_column;
    filer_column.resize_fill(block.rows(), 1);

    for (size_t i = 0; i < offsets.size(); ++i)
    {
        if (collision_offsets.find(i) != collision_offsets.end())
        {
            remove_count += getTokenEnd(i) - getTokenBegin(i);
            for (auto row_id = getTokenBegin(i); row_id < getTokenEnd(i); ++row_id)
                filer_column[row_id] = 0;
        }
        else
        {
            new_tokens->tokens.push_back(tokens[i]);
            new_tokens->offsets.push_back(new_tokens->getRows() + getTokenRows(i));
        }
    }

    chassert(remove_count > 0);
    chassert(remove_count <= block.rows());

    if (remove_count == block.rows())
    {
        new_tokens->original_block = std::make_shared<Block>(block.cloneEmpty());

        LOG_ERROR(
            logger,
            "All {} rows are removed due to duplicate, debug: {}",
            block.rows(),
            debug());

        return {.filtered_block = new_tokens->original_block, .deduplication_info = new_tokens, .removed_count = remove_count};
    }

    auto cols = block.getColumns();
    for (auto & col : cols)
        col = col->filter(filer_column, block.rows() - remove_count);

    Block filtered_block = block.cloneWithoutColumns();
    filtered_block.setColumns(cols);
    chassert(filtered_block.rows() == new_tokens->getRows());

    new_tokens->original_block = std::make_shared<Block>(std::move(filtered_block));

    LOG_ERROR(
        logger,
        "Removed {}/{} rows with {}/{} tokens due to duplicate, remaining rows/count={}/{}, debug: {}, new tokens debug: {}",
        remove_count,
        block.rows(),
        collision_offsets.size(),
        getCount(),
        new_tokens->original_block->rows(),
        new_tokens->getCount(),
        debug(),
        new_tokens->debug());

    return {.filtered_block = new_tokens->original_block, .deduplication_info = new_tokens, .removed_count = remove_count};
}


UInt128 DeduplicationInfo::calculateDataHash(size_t offset) const
{
    chassert(offset < offsets.size());
    chassert(original_block->rows() == getRows());

    auto cols = original_block->getColumns();

    SipHash hash;
    for (size_t j = getTokenBegin(offset); j < getTokenEnd(offset); ++j)
    {
        for (const auto & col : cols)
            col->updateHashWithValue(j, hash);
    }

    return hash.get128();
}

std::string DeduplicationInfo::TokenDefinition::getBlockId(const std::string & partition_id, HashCalculator get_data_hash, Level level_)
{
    // if user token is empty we calculate by_data_hash
    if (empty())
    {
        chassert(level_ == Level::SOURCE);
        const auto hash_value = get_data_hash();
        by_data = DB::toString(hash_value.items[0]) + "_" + DB::toString(hash_value.items[1]);
    }

    // only one value is set here
    auto extension = !by_data.empty() ? by_data : by_user;

    // for other token sources addition parts are appended
    for (const auto & extra : extra_tokens)
    {
        if (!by_data.empty() && (extra.type == Extra::SOURCE_ID || extra.type == Extra::SOURCE_NYMBER))
            continue; // source id is already included in by_data

        extension.append(":");
        extension.append(extra.toString());
    }

    SipHash hash;
    hash.update(extension.data(), extension.size());

    const auto hash_value = hash.get128();
    return partition_id + "_" + DB::toString(hash_value.items[0]) + "_" + DB::toString(hash_value.items[1]);
}


std::unordered_map<std::string, std::vector<size_t>> DeduplicationInfo::buildBlockIdToOffsetsMap(const std::string & partition_id) const
{
    std::unordered_map<std::string, std::vector<size_t>> result;

    auto block_ids = getBlockIds(partition_id, true);
    for (size_t i = 0; i < block_ids.size(); ++i)
    {
        result[block_ids[i]].push_back(i);
    }

    return result;
}


std::vector<std::string> DeduplicationInfo::getBlockIds(const std::string & partition_id, bool deduplication_enabled) const
{
    LOG_DEBUG(logger, "getBlockIds for partition_id={}, debug: {}", partition_id, debug());
    if (disabled || !deduplication_enabled)
        return {};

    std::vector<std::string> result;
    result.reserve(offsets.size());

    for (size_t i = 0; i < offsets.size(); ++i)
        result.push_back(
            tokens[i].getBlockId(
                partition_id,
                [this, i]() { return this->calculateDataHash(i); },
                level));

    LOG_DEBUG(logger, "getBlockIds {}, debug: {}", fmt::join(result, ", "), debug());
    return result;
}


size_t DeduplicationInfo::getCount() const
{
    return offsets.size();
}


size_t DeduplicationInfo::getRows() const
{
    if (offsets.empty())
        return 0;
    return offsets.back();
}


std::pair<std::string, size_t> DeduplicationInfo::debug(size_t offset) const
{
    chassert(offset < offsets.size());
    if (tokens[offset].empty())
        return {"-", getTokenEnd(offset)};
    else
        return {tokens[offset].by_user + tokens[offset].by_data, getTokenEnd(offset)};
}


std::string DeduplicationInfo::debug() const
{
    std::vector<std::string> token_strs;
    for (size_t i = 0; i < tokens.size(); ++i)
    {
        const auto & token = tokens[i];
        token_strs.push_back(token.debug());
        token_strs.back() += fmt::format("::{}", getTokenEnd(i));
    }

    if (token_strs.size() > 10)
    {
        token_strs.resize(10);
        token_strs.push_back("...");
    }

    std::string block_str;
    if (!original_block)
        block_str = "null";
    else if (original_block->empty())
        block_str = "empty";
    else
        block_str = fmt::format("rows/cols {}/{}", original_block->rows(), original_block->getColumns().size());

    return fmt::format(
        "inst: {}, {}, {}, level {}, rows/tokens {}/{}, in block: {}, tokens: {}:[{}], visited views: {}:[{}], chiosen partitions: <{}>",
        instance_id,
        is_async_insert ? "async" : "sync",
        disabled ? "disabled" : "enabled",
        level,
        getRows(), getCount(),
        block_str,
        getCount(), fmt::join(token_strs, ","),
        visited_views.size(), fmt::join(visited_views, ","),
        last_partition_choice);
}


DeduplicationInfo::Ptr DeduplicationInfo::create(bool async_insert_)
{
    struct make_shared_enabler : public DeduplicationInfo
    {
        explicit make_shared_enabler(bool async_insert)
            : DeduplicationInfo(async_insert)
        {}
    };
    return std::make_shared<make_shared_enabler>(async_insert_);
}


DeduplicationInfo::Ptr DeduplicationInfo::cloneSelf() const
{
    struct make_shared_enabler : public DeduplicationInfo
    {
        explicit make_shared_enabler(const DeduplicationInfo & inst)
            : DeduplicationInfo(inst)
        {}
    };
    return std::make_shared<make_shared_enabler>(*this);
}


ChunkInfo::Ptr DeduplicationInfo::clone() const
{
    return std::static_pointer_cast<ChunkInfo>(cloneSelf());
}


void DeduplicationInfo::setPartWriterHashForPartition(const std::string & hash, size_t count) const
{
    LOG_DEBUG(
        logger,
        "setPartWriterHashForPartition: hash={} count={}, debug: {}",
        hash,
        count,
        debug());

    if (level != Level::SOURCE)
        return;

    if (is_async_insert)
        return;

    chassert(getCount() >= 1);

    if (getCount() > 1)
        return;

    if (!tokens[0].empty())
        return;

    tokens[0].setDataToken(hash);
}


void DeduplicationInfo::setPartWriterHashes(const std::vector<std::string> & partitions_hashes, size_t count) const
{
    LOG_TRACE(
        logger,
        "setPartWriterHashes: token='{}' count={}, debug: {}",
        fmt::join(partitions_hashes, ","),
        count,
        debug());

    if (is_async_insert)
        return;

    if (level != Level::SOURCE)
        return;

    chassert(getCount() >= 1);

    if (getCount() > 1)
        return;

    if (!tokens[0].empty())
        return;

    tokens[0].setDataToken(fmt::format("all-partitions-{}", fmt::join(partitions_hashes, "-")));

    chassert(getRows() == count);
}


void DeduplicationInfo::redefineTokensWithDataHash()
{
    LOG_DEBUG(logger, "redefineTokensWithDataHash, debug: {}", debug());

    if (level != Level::SOURCE)
        return;

    for (size_t i = 0; i < tokens.size(); ++i)
    {
        auto & token = tokens[i];
        if (token.empty())
        {
            /// calculate tokens from data
            const auto hash_value = calculateDataHash(i);
            token.by_data = DB::toString(hash_value.items[0]) + "_" + DB::toString(hash_value.items[1]);
        }
    }
}


DeduplicationInfo::DeduplicationInfo(bool async_insert_)
    : instance_id(deduplication_info_id_counter.fetch_add(1))
    , is_async_insert(async_insert_)
{
    LOG_TRACE(logger, "Create DeduplicationInfo, debug: {}", debug());
}


DeduplicationInfo::DeduplicationInfo(const DeduplicationInfo & other)
    : instance_id(deduplication_info_id_counter.fetch_add(1))
    , is_async_insert(other.is_async_insert)
    , insert_dependencies(other.insert_dependencies)
    , disabled(other.disabled)
    , level(other.level)
    , tokens(other.tokens)
    , offsets(other.offsets)
    , original_block(other.original_block)
    , visited_views(other.visited_views)
    , last_partition_choice(other.last_partition_choice)
{
    LOG_TRACE(logger, "Clone DeduplicationInfo {} from {}", instance_id, other.debug());
}


void DeduplicationInfo::setUserToken(const String & token, size_t count)
{
    chassert(level == Level::SOURCE);

    tokens.push_back(TokenDefinition::asUserToken(token));
    offsets.push_back(getRows() + count);

    LOG_TRACE(logger, "setUserToken: token={} count={}, {}", token, count, debug());
}


DeduplicationInfo::TokenDefinition DeduplicationInfo::TokenDefinition::asUserToken(std::string token)
{
    TokenDefinition t;
    t.by_user = std::move(token);
    return t;
}


void DeduplicationInfo::TokenDefinition::setDataToken(std::string token)
{
    if (!empty())
        return;
    by_data = std::move(token);
}


namespace
{
using FilterPredicate = std::function<bool (const Chunk &)>;

template <typename Executor>
Chunk exec(QueryPipeline & pipeline, FilterPredicate predicate)
{
    auto executor = Executor(pipeline);
    bool is_done = false;
    Chunk chunk;
    while (!is_done && !predicate(chunk))
    {
        is_done = !executor.pull(chunk);
    }
    return chunk;
}
}


Block DeduplicationInfo::goRetry(SharedHeader && header, Chunk && filtered_data, Ptr filtered_info, ContextPtr context)
{
    auto builder = QueryPipelineBuilder();
    builder.init(Pipe(std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(filtered_data))));
    builder.addChain(insert_dependencies->createChainForDeduplicationRetry(*this));

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    chassert(pipeline.pulling());

    auto result_header = pipeline.getSharedHeader();

    auto filter =[this, filtered_info] (const Chunk & chunk) -> bool
    {
        if (!chunk)
            return false;

        auto info = chunk.getChunkInfos().getSafe<DeduplicationInfo>();

        LOG_DEBUG(this->logger, "examine chunk with deduplication info: {}", info->debug());
        return info->tokens == filtered_info->tokens;
    };

    auto result_chunk = context->getSettingsRef()[Setting::use_async_executor_for_materialized_views]
        ? exec<PullingAsyncPipelineExecutor>(pipeline, std::move(filter))
        : exec<PullingPipelineExecutor>(pipeline, std::move(filter));

    if (!result_chunk)
        return result_header->cloneEmpty();

    return result_header->cloneWithColumns(result_chunk.detachColumns());
}


void DeduplicationInfo::updateOriginalBlock(const Chunk & chunk, SharedHeader header)
{
    LOG_DEBUG(logger,
        "updateOriginalBlock with chunk rows/col {}/{} debug: {}",
        chunk.getNumRows(), chunk.getNumColumns(),
        debug());

    chassert(!visited_views.empty());

    original_block = std::make_shared<Block>(header->cloneWithColumns(chunk.getColumns()));
    original_block_view_id = visited_views.back();
}


void DeduplicationInfo::setInsertDependencies(InsertDependenciesBuilderConstPtr insert_dependencies_)
{
    insert_dependencies = std::move(insert_dependencies_);
}


void DeduplicationInfo::setRootViewID(const StorageIDMaybeEmpty & id)
{
    LOG_DEBUG(logger, "Setting root view ID '{}' in deduplication tokens", id);
    chassert(level == Level::SOURCE);
    if (!insert_dependencies || !insert_dependencies->deduplicate_blocks)
        disabled = true;

    if (!id.empty())
    {
        auto as_str = id.hasUUID() ? toString(id.uuid) : id.getFullNameNotQuoted();
        addExtraPart(TokenDefinition::Extra::asSourceID(as_str));
    }
    else
    {
        addExtraPart(TokenDefinition::Extra::asSourceID("empty"));
    }
    visited_views.push_back(id);
}


void DeduplicationInfo::setViewID(const StorageID & id)
{
    LOG_DEBUG(logger, "Setting view ID '{}', debug: {}", id, debug());

    if (level == Level::SOURCE)
        level = Level::VIEW;

    chassert(level == Level::VIEW);

    if (!insert_dependencies || !insert_dependencies->deduplicate_blocks_in_dependent_materialized_views)
        disabled = true;

    auto as_str = id.hasUUID() ? toString(id.uuid) : id.getFullNameNotQuoted();
    addExtraPart(TokenDefinition::Extra::asViewID(as_str));
    visited_views.push_back(id);
}


void DeduplicationInfo::rememberPartitionChoise(const std::string & partition_id)
{
    chassert(!visited_views.empty());
    LOG_DEBUG(
        logger,
        "Remembering partition choice '{}' for view '{}', debug: {}",
        partition_id,
        visited_views.back(),
        debug());
    last_partition_choice = partition_id;
}


void DeduplicationInfo::setViewBlockNumber(size_t block_number)
{
    chassert(level == Level::VIEW);
    addExtraPart(TokenDefinition::Extra::asViewNumber(block_number));
}


DeduplicationInfo::FilterResult DeduplicationInfo::deduplicateBlock(
    const std::vector<std::string> & existing_block_ids, const std::string & partition_id, ContextPtr context)
{
    chassert(level == Level::SOURCE);

    auto result = filterOriginalBlock(existing_block_ids, partition_id);
    chassert(result.filtered_block->rows() + result.removed_count == original_block->rows(), fmt::format(" {} + {} = {}",
        result.filtered_block->rows(), result.removed_count, original_block->rows()));
    chassert(result.removed_count > 0, "No rows were removed during deduplication");

    auto chunk = Chunk(result.filtered_block->getColumns(), result.filtered_block->rows());
    chunk.getChunkInfos().add(result.deduplication_info);

    auto header = std::make_shared<const Block>(result.filtered_block->cloneEmpty());

    auto block = goRetry(std::move(header), std::move(chunk), result.deduplication_info, context);
    result.filtered_block = std::make_shared<Block>(std::move(block));
    return result;
}


const std::vector<StorageIDMaybeEmpty> & DeduplicationInfo::getVisitedViews() const
{
    return visited_views;
}


const std::string & DeduplicationInfo::getLastPartitionChoice() const
{
    return last_partition_choice;
}


void DeduplicationInfo::setSourceBlockNumber(size_t block_number)
{
    chassert(level == Level::SOURCE);
    addExtraPart(TokenDefinition::Extra::asSourceNumber(block_number));
}


size_t DeduplicationInfo::getTokenBegin(size_t pos) const
{
    chassert(pos < offsets.size());
    if (pos == 0)
        return 0;
    return offsets[pos - 1];
}


size_t DeduplicationInfo::getTokenEnd(size_t pos) const
{
    chassert(pos < offsets.size());
    return offsets[pos];
}


size_t DeduplicationInfo::getTokenRows(size_t pos) const
{
    return getTokenEnd(pos) - getTokenBegin(pos);
}


void DeduplicationInfo::addExtraPart(const TokenDefinition::Extra & extra)
{
    for (auto & token : tokens)
        token.extra_tokens.push_back(extra);
}


bool DeduplicationInfo::TokenDefinition::empty() const
{
    return by_user.empty() && by_data.empty();
}


bool DeduplicationInfo::TokenDefinition::operator==(const TokenDefinition & other) const
{
    return by_user == other.by_user && by_data == other.by_data && extra_tokens == other.extra_tokens;
}


ChunkInfo::Ptr DeduplicationInfo::merge(const ChunkInfo::Ptr & right) const
{
    return std::static_pointer_cast<ChunkInfo>(mergeSelf(std::static_pointer_cast<DeduplicationInfo>(right)));
}


DeduplicationInfo::Ptr DeduplicationInfo::mergeSelf(const Ptr & right) const
{
    chassert(right);

    LOG_DEBUG(
        logger,
        "Merging:\n left: {}\n right: {}\n"
        , debug()
        , right->debug());

    chassert(is_async_insert == right->is_async_insert);
    chassert(this->visited_views == right->visited_views);

    if (!disabled && is_async_insert && visited_views.size() > 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Merging deduplication infos with more than one visited view is not supported with deduplication on async insert, left: {}, right: {}",
            debug(),
            right->debug());

    if (!disabled && !is_async_insert && visited_views.size() > 2)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Merging deduplication infos with more than two visited view is not supported with deduplication on sync insert, left: {}, right: {}",
            debug(),
            right->debug());

    auto new_instance = DeduplicationInfo::create(is_async_insert);
    new_instance->level = level;
    new_instance->visited_views = this->visited_views;
    new_instance->last_partition_choice = this->last_partition_choice;
    new_instance->insert_dependencies = this->insert_dependencies;

    auto do_extend = [&] ()
    {
        chassert(this->getCount() == 1);
        chassert(right->getCount() == 1);
        new_instance->tokens.push_back(this->tokens[0]);
        new_instance->tokens.back().doExtend(right->tokens[0]);
        new_instance->offsets.push_back(this->getRows() + right->getRows());
    };

    auto do_concat = [&] ()
    {
        // concat tokens
        new_instance->tokens.reserve(this->tokens.size() + right->tokens.size());
        new_instance->tokens.insert(new_instance->tokens.end(), this->tokens.begin(), this->tokens.end());
        new_instance->tokens.insert(new_instance->tokens.end(), right->tokens.begin(), right->tokens.end());

        // concat offsets
        new_instance->offsets.reserve(this->offsets.size() + right->offsets.size());
        new_instance->offsets.insert(new_instance->offsets.end(), this->offsets.begin(), this->offsets.end());
        size_t rows = new_instance->getRows(); // correct offset for right part
        for (const auto & offset : right->offsets)
            new_instance->offsets.push_back(rows + offset);

        chassert(new_instance->tokens.size() == new_instance->offsets.size());
    };

    if (this->getCount() == 1 && right->getCount() == 1 && this->tokens[0].canBeExtended(right->tokens[0]))
    {
        // this is a sqush when both sides have the same token for all rows
        do_extend();
    }
    else
    {
        do_concat();
    }

    LOG_DEBUG(
        logger,
        "Merged: {}",
        new_instance->debug());

    return new_instance;
}


std::string DeduplicationInfo::TokenDefinition::Extra::toString() const
{
    switch (type)
    {
        case SOURCE_ID:
            return "source-id-" + std::get<String>(value_variant);
        case SOURCE_NYMBER:
            {
                const auto & range = std::get<Range>(value_variant);
                if (range.first + 1 == range.second)
                    return "source-number-" + DB::toString(range.first);
                else
                    return "source-number-" + DB::toString(range.first) + "-" + DB::toString(range.second);
            }
        case VIEW_ID:
            return "view-id-" + std::get<String>(value_variant);
        case VIEW_NUMBER:
            {
                const auto & range = std::get<Range>(value_variant);
                if (range.first + 1 == range.second)
                    return "view-number-" + DB::toString(range.first);
                else
                    return "view-number-" + DB::toString(range.first) + "-" + DB::toString(range.second);
            }
    }
    UNREACHABLE();
}

std::string DeduplicationInfo::TokenDefinition::debug() const
{
    std::string str;

    if (!by_user.empty())
        str = "user<" + by_user + ">";
    else if (!by_data.empty())
        str = "data-hash<" + by_data + ">";
    else
        str = "-";

    for (const auto & extra : extra_tokens)
    {
        str += "/";
        str += extra.toString();
    }

    return str;
}
bool DeduplicationInfo::TokenDefinition::Extra::operator==(const Extra & other) const
{
    return type == other.type && value_variant == other.value_variant;
}
DeduplicationInfo::TokenDefinition::Extra DeduplicationInfo::TokenDefinition::Extra::asSourceID(const String & id)
{
    return Extra{SOURCE_ID, id};
}
DeduplicationInfo::TokenDefinition::Extra DeduplicationInfo::TokenDefinition::Extra::asSourceNumber(uint64_t number)
{
    return Extra{SOURCE_NYMBER, Range{number, number + 1}};
}
DeduplicationInfo::TokenDefinition::Extra DeduplicationInfo::TokenDefinition::Extra::asViewID(const String & id)
{
    return Extra{VIEW_ID, id};
}
DeduplicationInfo::TokenDefinition::Extra DeduplicationInfo::TokenDefinition::Extra::asViewNumber(uint64_t number)
{
    return Extra{VIEW_NUMBER, Range{number, number + 1}};
}
bool DeduplicationInfo::TokenDefinition::canBeExtended(const TokenDefinition & right) const
{
    LOG_DEBUG(getLogger("canBeExtended"), "{} vs {}", this->debug(), right.debug());

    if (by_user != right.by_user || by_data != right.by_data)
        return false;

    if (extra_tokens.size() != right.extra_tokens.size())
        return false;

    if (extra_tokens.empty())
        return true;

    const auto & left_last_extra = extra_tokens.back();
    const auto & right_last_extra = right.extra_tokens.back();

    if (left_last_extra.type != right_last_extra.type)
        return false;

    if (left_last_extra != right_last_extra)
    {
        // type is equal but values are different

        switch (left_last_extra.type)
        {
            case Extra::Type::SOURCE_ID:
            case Extra::Type::VIEW_ID:
                return false;
            case Extra::Type::SOURCE_NYMBER:
            case Extra::Type::VIEW_NUMBER: {
                const auto & left_range = std::get<Extra::Range>(left_last_extra.value_variant);
                const auto & right_range = std::get<Extra::Range>(right_last_extra.value_variant);
                // check if ranges are continuous
                if (left_range.second == right_range.first)
                    break; // continue and check other extras
                return false;
            }
        }
    }

    for (size_t i = 0; i < extra_tokens.size() - 1; ++i)
    {
        if (extra_tokens[i] != right.extra_tokens[i])
            return false;
    }

    return true;
}
void DeduplicationInfo::TokenDefinition::doExtend(const TokenDefinition & right)
{
    chassert(canBeExtended(right));

    if (extra_tokens.empty())
        return;

    auto & left_last_extra = extra_tokens.back();
    const auto & right_last_extra = right.extra_tokens.back();

    if (left_last_extra == right_last_extra)
        return;

    // type is equal but values are different
    switch (left_last_extra.type)
    {
        case Extra::Type::SOURCE_ID:
        case Extra::Type::VIEW_ID:
            chassert(false);
            break;
        case Extra::Type::SOURCE_NYMBER:
        case Extra::Type::VIEW_NUMBER: {
            auto & left_range = std::get<Extra::Range>(left_last_extra.value_variant);
            const auto & right_range = std::get<Extra::Range>(right_last_extra.value_variant);
            // extend range
            chassert(left_range.second == right_range.first);
            left_range.second = right_range.second;
            break;
        }
    }
}
}
