//
// Created by marcel on 27.08.21.
//
#pragma once
#ifndef THRILL_CORE_REDUCE_CUCKOO_HASH_TABLE_HEADER
#define THRILL_CORE_REDUCE_CUCKOO_HASH_TABLE_HEADER

#include <thrill/core/reduce_functional.hpp>
#include <thrill/core/reduce_table.hpp>

namespace thrill {
namespace core {

template <typename TableItem, typename Key, typename Value,
        typename KeyExtractor, typename ReduceFunction, typename Emitter,
        const bool VolatileKey,
        typename ReduceConfig,
        typename IndexFunction,
        typename KeyEqualFunction = std::equal_to<Key> >
class ReduceCuckooHashTable
    : public ReduceTable<TableItem, Key, Value,
            KeyExtractor, ReduceFunction, Emitter,
            VolatileKey, ReduceConfig,
            IndexFunction, KeyEqualFunction>
{
    using Super = ReduceTable<TableItem, Key, Value,
            KeyExtractor, ReduceFunction, Emitter,
            VolatileKey, ReduceConfig, IndexFunction,
            KeyEqualFunction>;

    using Super::debug;
    static constexpr bool debug_items = false;

    //! target number of bytes in a BucketBlock.
    static constexpr size_t bucket_block_size
            = ReduceConfig::cuckoo_block_size_;

    static constexpr int max_displacement_cycles
            = ReduceConfig::max_displacement_cycles_;

    static constexpr int number_of_hashes
            = ReduceConfig::num_hashes_;

public:
    //! calculate number of items such that each BucketBlock has about 1 MiB of
    //! size, or at least 8 items.
    static constexpr size_t block_size_ =
            common::max<size_t>(1, bucket_block_size / sizeof(TableItem));

    //! Block holding reduce key/value pairs.
    struct BucketBlock {
        //! number of _used_/constructed items in this block. next is unused if
        //! size != block_size.
        size_t size;

        //! link of linked list to next block
        BucketBlock *next;

        //! memory area of items
        TableItem items[block_size_]; // NOLINT

        //! helper to destroy all allocated items
        void destroy_items() {
            for (TableItem *i = items; i != items + size; ++i) {
                i->~TableItem();
            }
        }
    };

    using BucketBlockIterator = typename std::vector<BucketBlock*>::iterator;

public:

    ReduceCuckooHashTable(
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

        limit_memory_bytes_ = limit_memory_bytes;

        // calculate maximum number of blocks allowed in a partition due to the
        // memory limit.

        assert(limit_memory_bytes_ >= 0 &&
               "limit_memory_bytes must be greater than or equal to 0. "
               "a byte size of zero results in exactly one item per partition");

        max_blocks_per_partition_ = std::max<size_t>(
                1,
                (size_t)(static_cast<double>(limit_memory_bytes_)
                         / static_cast<double>(num_partitions_)
                         / static_cast<double>(sizeof(BucketBlock))));

        assert(max_blocks_per_partition_ > 0);

        // calculate limit on the number of _items_ in a partition before these
        // are spilled to disk or flushed to network.

        double limit_fill_rate = config_.limit_partition_fill_rate();

        assert(limit_fill_rate >= 0.0 && limit_fill_rate <= 1.0
               && "limit_partition_fill_rate must be between 0.0 and 1.0. "
                  "with a fill rate of 0.0, items are immediately flushed.");

        max_items_per_partition_ = max_blocks_per_partition_ * block_size_;

        limit_items_per_partition_ = (size_t)(
                static_cast<double>(max_items_per_partition_) * limit_fill_rate);

        assert(max_items_per_partition_ > 0);
        assert(limit_items_per_partition_ >= 0);

        // calculate number of slots in a partition of the bucket table, i.e.,
        // the number of bucket pointers per partition

        double bucket_rate = config_.bucket_rate();

        assert(bucket_rate >= 0.0 &&
               "bucket_rate must be greater than or equal 0. "
               "a bucket rate of 0.0 causes exactly 1 bucket per partition.");

        num_buckets_per_partition_ = std::max<size_t>(
                1,
                (size_t)(static_cast<double>(max_blocks_per_partition_)
                         * bucket_rate));

        assert(num_buckets_per_partition_ > 0);

        // reduce max number of blocks per partition to cope for the memory
        // needed for pointers

        max_blocks_per_partition_ -= std::max<size_t>(
                0,
                (size_t)(std::ceil(
                        static_cast<double>(
                                num_buckets_per_partition_ * sizeof(BucketBlock*))
                        / static_cast<double>(sizeof(BucketBlock)))));

        max_blocks_per_partition_ = std::max<size_t>(max_blocks_per_partition_, 1);

        // finally, calculate number of buckets and allocate the table

        num_buckets_ = num_buckets_per_partition_ * num_partitions_;
        limit_blocks_ = max_blocks_per_partition_ * num_partitions_;

        assert(num_buckets_ > 0);
        assert(limit_blocks_ > 0);

        sLOG << "num_partitions_" << num_partitions_
             << "num_buckets_per_partition_" << num_buckets_per_partition_
             << "num_buckets_" << num_buckets_;

        buckets_.resize(num_buckets_, nullptr);
    }

    //! non-copyable: delete copy-constructor
    ReduceCuckooHashTable(const ReduceCuckooHashTable&) = delete;
    //! non-copyable: delete assignment operator
    ReduceCuckooHashTable& operator = (const ReduceCuckooHashTable&) = delete;

    ~ReduceCuckooHashTable() {
        Dispose();
    }

    bool Insert(const TableItem& kv) {
        while (TLX_UNLIKELY(mem::memory_exceeded && num_items_ != 0))
            SpillAnyPartition();

        if (CanBeReduced(kv)) return false;

        TableItem insert_element = kv;
        TableItem tmp_;
        int hash = 0;

        for (int i = 0; i < max_displacement_cycles; ++i) {
            typename IndexFunction::Result h = calculate_index(insert_element, hash);

            size_t local_index = h.local_index(num_buckets_per_partition_);

            assert(h.partition_id < num_partitions_);
            assert(local_index < num_buckets_per_partition_);

            size_t global_index =
                    h.partition_id * num_buckets_per_partition_ + local_index;
            BucketBlock* current = buckets_[global_index];

            if (current == nullptr)
            {
                // new block needed.

                // flush largest partition if max number of blocks reached
                while (num_blocks_ > limit_blocks_)
                    SpillAnyPartition();

                // allocate a new block of uninitialized items, prepend to bucket
                current = block_pool_.GetBlock();
                current->next = buckets_[global_index];
                buckets_[global_index] = current;

                // Total number of blocks
                ++num_blocks_;
            }

            if (current->size == block_size_)
            {
                std::swap(*(current->items),tmp_);
                new (current->items)TableItem(insert_element);
                insert_element = tmp_;
                hash=(hash+1)%number_of_hashes;
            } else {
                // in-place construct/insert new item in current bucket block
                new (current->items + current->size++)TableItem(insert_element);

                LOGC(debug_items)
                << "h.partition_id" << h.partition_id;

                // Increase partition item count
                ++items_per_partition_[h.partition_id];
                ++num_items_;

                LOGC(debug_items)
                << "items_per_partition_[" << h.partition_id << "]"
                << items_per_partition_[h.partition_id];

                // flush current partition if max partition fill rate reached
                while (items_per_partition_[h.partition_id] > limit_items_per_partition_)
                    SpillPartition(h.partition_id);

                return true;
            }
        }

        // Loop detected, flush items and try again
        SpillAnyPartition();

        return Insert(insert_element);
    }

    bool CanBeReduced(const TableItem& kv) {
        typename IndexFunction::Result h;

        for(int i = 0; i < number_of_hashes; i++)
        {
            h = calculate_index(kv, i);

            size_t local_index = h.local_index(num_buckets_per_partition_);

            assert(h.partition_id < num_partitions_);
            assert(local_index < num_buckets_per_partition_);

            size_t global_index =
                    h.partition_id * num_buckets_per_partition_ + local_index;
            BucketBlock* current = buckets_[global_index];

            while (current != nullptr)
            {
                // iterate over valid items in a block
                for (TableItem* bi = current->items;
                     bi != current->items + current->size; ++bi)
                {
                    // if item and key equals, then reduce.
                    if (key_equal_function_(key(kv), key(*bi)))
                    {
                        *bi = reduce(*bi, kv);
                        return true;
                    }
                }
                current = current->next;
            }
        }
        return false;
    }

    //! Deallocate memory
    void Dispose() {
        // destroy all block chains
        for (BucketBlock* b_block : buckets_)
        {
            BucketBlock* current = b_block;
            while (current != nullptr)
            {
                // destroy block and advance to next
                BucketBlock* next = current->next;
                current->destroy_items();
                operator delete (current);
                current = next;
            }
        }

        // destroy vector and block pool
        tlx::vector_free(buckets_);
        block_pool_.Destroy();

        Super::Dispose();
    }

    //! Spill all items of an arbitrary partition into an external memory File.
    void SpillAnyPartition() {
        return SpillLargestPartition();
    }

    void SpillPartition(size_t partition_id) {

        if (immediate_flush_) {
            return FlushPartition(
                    partition_id, /* consume */ true, /* grow */ true);
        }

        sLOG << "Spilling" << items_per_partition_[partition_id]
             << "items of partition" << partition_id
             << "buckets: [" << partition_id * num_buckets_per_partition_
             << "," << (partition_id + 1) * num_buckets_per_partition_ << ")";

        if (items_per_partition_[partition_id] == 0)
            return;

        data::File::Writer writer = partition_files_[partition_id].GetWriter();

        BucketBlockIterator iter =
                buckets_.begin() + partition_id * num_buckets_per_partition_;
        BucketBlockIterator end =
                buckets_.begin() + (partition_id + 1) * num_buckets_per_partition_;

        for ( ; iter != end; ++iter)
        {
            BucketBlock* current = *iter;

            while (current != nullptr)
            {
                for (TableItem* bi = current->items;
                     bi != current->items + current->size; ++bi)
                {
                    writer.Put(*bi);
                }

                // destroy block and advance to next
                BucketBlock* next = current->next;
                block_pool_.Deallocate(current);
                --num_blocks_;
                current = next;
            }

            *iter = nullptr;
        }

        // reset partition specific counter
        num_items_ -= items_per_partition_[partition_id];
        items_per_partition_[partition_id] = 0;
        assert(num_items_ == this->num_items_calc());

        sLOG << "Spilled items of partition" << partition_id;
    }

    //! Spill all items of the largest partition into an external memory File.
    void SpillLargestPartition() {
        // get partition with max size
        size_t size_max = 0, index = 0;

        for (size_t i = 0; i < num_partitions_; ++i)
        {
            if (items_per_partition_[i] > size_max)
            {
                size_max = items_per_partition_[i];
                index = i;
            }
        }

        if (size_max == 0) {
            return;
        }

        return SpillPartition(index);
    }

    template <typename Emit>
    void FlushPartitionEmit(
            size_t partition_id, bool consume, bool /* grow */, Emit emit) {

            LOG << "Flushing " << items_per_partition_[partition_id]
                << " items of partition: " << partition_id;

            if (items_per_partition_[partition_id] == 0) return;

            BucketBlockIterator iter =
                    buckets_.begin() + partition_id * num_buckets_per_partition_;
            BucketBlockIterator end =
                    buckets_.begin() + (partition_id + 1) * num_buckets_per_partition_;

            for ( ; iter != end; ++iter)
            {
                BucketBlock* current = *iter;

                while (current != nullptr)
                {
                    for (TableItem* bi = current->items;
                         bi != current->items + current->size; ++bi)
                    {
                        emit(partition_id, *bi);
                    }

                    if (consume) {
                        // destroy block and advance to next
                        BucketBlock* next = current->next;
                        block_pool_.Deallocate(current);
                        --num_blocks_;
                        current = next;
                    }
                    else {
                        // advance to next
                        current = current->next;
                    }
                }

                if (consume)
                    *iter = nullptr;
            }

            if (consume) {
                // reset partition specific counter
                num_items_ -= items_per_partition_[partition_id];
                items_per_partition_[partition_id] = 0;
                assert(num_items_ == this->num_items_calc());
            }

            LOG << "Done flushing items of partition: " << partition_id;
    }

    void FlushPartition(size_t partition_id, bool consume, bool grow) {
        FlushPartitionEmit(
                partition_id, consume, grow,
                [this](const size_t& partition_id, const TableItem& p) {
                    this->emitter_.Emit(partition_id, p);
                });
    }

    void FlushAll() {
        for (size_t i = 0; i < num_partitions_; ++i) {
            FlushPartition(i, /* consume */ true, /* grow */ false);
        }
    }

protected:
    //! BucketBlockPool to stack allocated BucketBlocks
    class BucketBlockPool
    {
    public:
        BucketBlockPool() = default;

        //! non-copyable: delete copy-constructor
        BucketBlockPool(const BucketBlockPool&) = delete;
        //! non-copyable: delete assignment operator
        BucketBlockPool& operator = (const BucketBlockPool&) = delete;
        //! move-constructor: default
        BucketBlockPool(BucketBlockPool&&) = default;
        //! move-assignment operator: default
        BucketBlockPool& operator = (BucketBlockPool&&) = default;

        ~BucketBlockPool() {
            Destroy();
        }

        // allocate a chunk of memory as big as Type needs:
        BucketBlock * GetBlock() {
            BucketBlock* place;
            if (!free.empty()) {
                place = static_cast<BucketBlock*>(free.top());
                free.pop();
            }
            else {
                place = static_cast<BucketBlock*>(operator new (sizeof(BucketBlock)));
                place->size = 0;
                place->next = nullptr;
            }

            return place;
        }

        // mark some memory as available (no longer used):
        void Deallocate(BucketBlock* o) {
            o->size = 0;
            o->next = nullptr;
            free.push(static_cast<BucketBlock*>(o));
        }

        void Destroy() {
            while (!free.empty()) {
                free.top()->destroy_items();
                operator delete (free.top());
                free.pop();
            }
        }

    private:
        // stack to hold pointers to free chunks:
        std::stack<BucketBlock*> free;
    };

public:
    using Super::calculate_index;

private:
    using Super::config_;
    using Super::immediate_flush_;
    using Super::index_function_;
    using Super::items_per_partition_;
    using Super::key;
    using Super::key_equal_function_;
    using Super::limit_items_per_partition_;
    using Super::limit_memory_bytes_;
    using Super::num_buckets_;
    using Super::num_buckets_per_partition_;
    using Super::num_items_;
    using Super::num_partitions_;
    using Super::partition_files_;
    using Super::reduce;

    //! Storing the items.
    std::vector<BucketBlock*> buckets_;

    //! Bucket block pool.
    BucketBlockPool block_pool_;

    //! Number of blocks in the table before some items are spilled.
    size_t limit_blocks_;

    //! Maximal number of items per partition.
    size_t max_items_per_partition_;

    //! Maximal number of blocks per partition.
    size_t max_blocks_per_partition_;

    //! Total number of blocks in the table.
    size_t num_blocks_ = 0;
};

template <typename TableItem, typename Key, typename Value,
        typename KeyExtractor, typename ReduceFunction,
        typename Emitter, const bool VolatileKey,
        typename ReduceConfig, typename IndexFunction,
        typename KeyEqualFunction>
class ReduceTableSelect<
        ReduceTableImpl::CUCKOO,
        TableItem, Key, Value, KeyExtractor, ReduceFunction,
        Emitter, VolatileKey, ReduceConfig, IndexFunction, KeyEqualFunction>
{
public:
    using type = ReduceCuckooHashTable<
            TableItem, Key, Value, KeyExtractor, ReduceFunction,
            Emitter, VolatileKey, ReduceConfig,
            IndexFunction, KeyEqualFunction>;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_CUCKOO_HASH_TABLE_HEADER
