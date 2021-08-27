//
// Created by marcel on 27.08.21.
//
#pragma once
#ifndef THRILL_CORE_REDUCE_DYSECT_HASH_TABLE_HEADER
#define THRILL_CORE_REDUCE_DYSECT_HASH_TABLE_HEADER

#include <thrill/core/reduce_functional.hpp>
#include <thrill/core/reduce_table.hpp>

namespace thrill {
namespace core {

template <typename TableItem, typename Key, typename Value,
        typename KeyExtractor, typename ReduceFunction, typename Emitter,
        const bool VolatileKey,
        typename ReduceConfig_,
        typename IndexFunction,
        typename KeyEqualFunction = std::equal_to<Key> >
class ReduceDysectHashTable
    : public ReduceTable<TableItem, Key, Value,
            KeyExtractor, ReduceFunction, Emitter,
            VolatileKey, ReduceConfig_,
            IndexFunction, KeyEqualFunction>
{
    using Super = ReduceTable<TableItem, Key, Value,
            KeyExtractor, ReduceFunction, Emitter,
            VolatileKey, ReduceConfig_, IndexFunction,
            KeyEqualFunction>;
    using Super::debug;

public:
    using ReduceConfig = ReduceConfig_;

    ReduceDysectHashTable(
            Context& ctx, size_t dia_id,
            const KeyExtractor& key_extractor,
            const ReduceFunction& reduce_function,
            Emitter& emitter,
            size_t num_partitions,
            const ReduceConfig& config = ReduceConfig(),
            bool immediate_flush = false,
            const IndexFunction& index_function = IndexFunction(),
            const KeyEqualFunction& key_equal_function = KeyEqualFunction())
            : Super(ctx, dia_id,
                    key_extractor, reduce_function, emitter,
                    num_partitions, config, immediate_flush,
                    index_function, key_equal_function)
    { assert(num_partitions > 0); }

    void Initialize(size_t limit_memory_bytes) {
        LOG << "Initialized with memory limit bytes: " << limit_memory_bytes;
    }

    ~ReduceDysectHashTable() {
        Dispose();
    }

    bool Insert(const TableItem& kv) {
        return true;
    }

    //! Deallocate memory
    void Dispose() {
        Super::Dispose();
    }

    void SpillPartition(size_t partition_id) {
        LOG << "Spilling Partition with ID: " << partition_id;
    }

    template <typename Emit>
    void FlushPartitionEmit(
            size_t partition_id, bool consume, bool grow, Emit emit) {
        LOG << "Flush Partition with ID: " << partition_id
            << " with consume " << consume << " grow: " << grow;
    }

    void FlushPartition(size_t partition_id, bool consume, bool grow) {
        FlushPartitionEmit(
                partition_id, consume, grow,
                [this](const size_t& partition_id, const TableItem& p) {
                    this->emitter_.Emit(partition_id, p);
                });
    }

public:
    using Super::calculate_index;

private:
    using Super::config_;
    using Super::immediate_flush_;
    using Super::index_function_;
    using Super::items_per_partition_;
    using Super::key;
    using Super::key_equal_function_;
    using Super::limit_memory_bytes_;
    using Super::num_buckets_;
    using Super::num_buckets_per_partition_;
    using Super::num_items_;
    using Super::num_partitions_;
    using Super::partition_files_;
    using Super::reduce;
};

template <typename TableItem, typename Key, typename Value,
        typename KeyExtractor, typename ReduceFunction,
        typename Emitter, const bool VolatileKey,
        typename ReduceConfig, typename IndexFunction,
        typename KeyEqualFunction>
class ReduceTableSelect<
        ReduceTableImpl::DySECT,
        TableItem, Key, Value, KeyExtractor, ReduceFunction,
        Emitter, VolatileKey, ReduceConfig, IndexFunction, KeyEqualFunction>
{
public:
    using type = ReduceDysectHashTable<
            TableItem, Key, Value, KeyExtractor, ReduceFunction,
            Emitter, VolatileKey, ReduceConfig,
            IndexFunction, KeyEqualFunction>;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_DYSECT_HASH_TABLE_HEADER
