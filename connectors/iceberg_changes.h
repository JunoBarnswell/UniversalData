#ifndef UD_ICEBERG_CHANGES_H
#define UD_ICEBERG_CHANGES_H

#include "ud/data.h"

#include <new>
#include <span>
#include <unordered_map>

/* A v2 equality delete applies to earlier snapshots. Collapse this batch's
 * mutations separately so an overwritten or deleted row never reaches the
 * batch's own data files. Preserve equality deletes for the prior snapshots. */
inline ud_status ud_iceberg_select_current_rows(const ud_batch* rows, const ud_batch* before,
                                                std::span<const uint32_t> keys,
                                                std::span<uint8_t> insert_rows,
                                                std::span<const uint8_t> delete_rows) try {
    const uint32_t count = ud_batch_rows(rows);
    std::unordered_multimap<uint64_t, uint32_t> live;
    live.reserve(count);

    auto discard_prior = [&](const ud_batch* source, uint32_t row, uint64_t& hash) -> ud_status {
        hash = UD_VALUE_HASH_SEED;
        for (uint32_t key : keys) {
            ud_value value{};
            ud_status status = ud_batch_get(source, row, key, &value);
            if (status) return status;
            if (value.is_null) return UD_SCHEMA;
            hash = ud_value_hash(&value, hash);
        }
        const auto [first, last] = live.equal_range(hash);
        for (auto candidate = first; candidate != last; ++candidate) {
            bool equal = true;
            for (uint32_t key : keys) {
                ud_value old_value{}, new_value{};
                ud_status status = ud_batch_get(rows, candidate->second, key, &old_value);
                if (!status) status = ud_batch_get(source, row, key, &new_value);
                if (status) return status;
                if (ud_value_compare(&old_value, &new_value) != 0) {
                    equal = false;
                    break;
                }
            }
            if (equal) {
                insert_rows[candidate->second] = 0;
                live.erase(candidate);
                break;
            }
        }
        return UD_OK;
    };

    for (uint32_t row = 0; row < count; ++row) {
        uint64_t hash;
        if (delete_rows[row]) {
            ud_status status = discard_prior(before ? before : rows, row, hash);
            if (status) return status;
        }
        if (insert_rows[row]) {
            ud_status status = discard_prior(rows, row, hash);
            if (status) return status;
            live.emplace(hash, row);
        }
    }
    return UD_OK;
} catch (const std::bad_alloc&) {
    return UD_RESOURCE;
}

#endif
