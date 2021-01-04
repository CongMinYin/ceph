// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ReplicatedWriteLog.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "include/ceph_assert.h"
#include "common/deleter.h"
#include "common/dout.h"
#include "common/environment.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "common/Timer.h"
#include "common/perf_counters.h"
#include "librbd/ImageCtx.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/cache/pwl/ImageCacheState.h"
#include "librbd/cache/pwl/LogEntry.h"
#include "librbd/plugin/Api.h"
#include "librbd/cache/pwl/ReplicatedDataPool.h"
#include <map>
#include <vector>

#undef dout_subsys
#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::ReplicatedWriteLog: " << this << " " \
                             <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace pwl {

using namespace librbd::cache::pwl;

const unsigned long int OPS_APPENDED_TOGETHER = MAX_ALLOC_PER_TRANSACTION;

template <typename I>
ReplicatedWriteLog<I>::ReplicatedWriteLog(
    I &image_ctx, librbd::cache::pwl::ImageCacheState<I>* cache_state,
    ImageWritebackInterface& image_writeback,
    plugin::Api<I>& plugin_api)
: AbstractWriteLog<I>(image_ctx, cache_state, image_writeback, plugin_api)
{ 
}

template <typename I>
ReplicatedWriteLog<I>::~ReplicatedWriteLog() {
  m_log_pool = nullptr;
}

/*
 * Allocate the (already reserved) write log entries for a set of operations.
 *
 * Locking:
 * Acquires lock
 */
template <typename I>
void ReplicatedWriteLog<I>::alloc_op_log_entries(GenericLogOperations &ops)
{
  struct WriteLogPmemEntry *pmem_log_entries = m_log_pool->get_log_entries();
  
  ceph_assert(ceph_mutex_is_locked_by_me(this->m_log_append_lock));
  
  /* Allocate the (already reserved) log entries */
  std::lock_guard locker(m_lock);
  
  for (auto &operation : ops) {
    uint32_t entry_index = this->m_first_free_entry;
    this->m_first_free_entry = (this->m_first_free_entry + 1) % this->m_total_log_entries;
    auto &log_entry = operation->get_log_entry();
    log_entry->log_entry_index = entry_index;
    log_entry->ram_entry.entry_index = entry_index;
    log_entry->pmem_entry = &pmem_log_entries[entry_index];
    log_entry->ram_entry.entry_valid = 1;
    m_log_entries.push_back(log_entry);
    ldout(m_image_ctx.cct, 20) << "operation=[" << *operation << "]" << dendl;
  } 
}

/*
 * Write and persist the (already allocated) write log entries and
 * data buffer allocations for a set of ops. The data buffer for each
 * of these must already have been persisted to its reserved area.
 */
template <typename I>
int ReplicatedWriteLog<I>::append_op_log_entries(GenericLogOperations &ops)
{
  GenericLogOperationsVector entries_to_flush;
  int ret = 0;

  ceph_assert(ceph_mutex_is_locked_by_me(this->m_log_append_lock));

  if (ops.empty()) {
    return 0;
  }
  entries_to_flush.reserve(OPS_APPENDED_TOGETHER);

  /* Write log entries to ring and persist */
  utime_t now = ceph_clock_now();
  for (auto &operation : ops) {
    if (!entries_to_flush.empty()) {
      /* Flush these and reset the list if the current entry wraps to the
       * tail of the ring */
      if (entries_to_flush.back()->get_log_entry()->log_entry_index >
          operation->get_log_entry()->log_entry_index) {
        ldout(m_image_ctx.cct, 20) << "entries to flush wrap around the end of the ring at "
                                   << "operation=[" << *operation << "]" << dendl;
        flush_op_log_entries(entries_to_flush);
        entries_to_flush.clear();
        now = ceph_clock_now();
      } 
    } 
    ldout(m_image_ctx.cct, 20) << "Copying entry for operation at index="
                               << operation->get_log_entry()->log_entry_index << " "
                               << "from " << &operation->get_log_entry()->ram_entry << " "
                               << "to " << operation->get_log_entry()->pmem_entry << " "
                               << "operation=[" << *operation << "]" << dendl;
    ldout(m_image_ctx.cct, 05) << "APPENDING: index="
                               << operation->get_log_entry()->log_entry_index << " "
                               << "operation=[" << *operation << "]" << dendl;
    operation->log_append_time = now;
    *operation->get_log_entry()->pmem_entry = operation->get_log_entry()->ram_entry;
    ldout(m_image_ctx.cct, 20) << "APPENDING: index="
                               << operation->get_log_entry()->log_entry_index << " "
                               << "pmem_entry=[" << *operation->get_log_entry()->pmem_entry
                               << "]" << dendl;
    entries_to_flush.push_back(operation);
  } 
  flush_op_log_entries(entries_to_flush);

  /*
   * Atomically advance the log head pointer and publish the
   * allocations for all the data buffers they refer to.
   */
  utime_t tx_start = ceph_clock_now();
  m_log_pool->update_first_free_entry(m_first_free_entry);

  utime_t tx_end = ceph_clock_now();
  m_perfcounter->tinc(l_librbd_pwl_append_tx_t, tx_end - tx_start);
  m_perfcounter->hinc(
    l_librbd_pwl_append_tx_t_hist, utime_t(tx_end - tx_start).to_nsec(), ops.size());
  for (auto &operation : ops) {
    operation->log_append_comp_time = tx_end;
  } 

  return ret;
}

/*
 * Flush the persistent write log entries set of ops. The entries must
 * be contiguous in persistent memory.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_op_log_entries(GenericLogOperationsVector &ops)
{
  if (ops.empty()) {
    return;
  } 

  if (ops.size() > 1) {
    ceph_assert(ops.front()->get_log_entry()->pmem_entry < ops.back()->get_log_entry()->pmem_entry);
  } 

  ldout(m_image_ctx.cct, 20) << "entry count=" << ops.size() << " "
                             << "start address="
                             << ops.front()->get_log_entry()->pmem_entry << " "
                             << "bytes="
                             << ops.size() * sizeof(*(ops.front()->get_log_entry()->pmem_entry))
                             << dendl;
  m_log_pool->flush_log_entries(
    ops.front()->get_log_entry()->log_entry_index,
    ops.size());
}

template <typename I>
void ReplicatedWriteLog<I>::remove_pool_file() {
  ldout(m_image_ctx.cct, 5) << "Removing empty pool file: " << this->m_log_pool_name << dendl;

  ceph_assert(m_log_pool);
 
  int rs = 0; 
  if (m_cache_state->clean) {
    rs = m_log_pool->close(true);
  } else {
    rs = m_log_pool->close(false);
    ldout(m_image_ctx.cct, 5) << "Not removing pool file: " << this->m_log_pool_name << dendl;
  }
  if (rs == 0) {
    m_cache_state->clean = true;
    m_cache_state->empty = true;
    m_cache_state->present = false;
  } else {
    lderr(m_image_ctx.cct) << "failed to remove empty pool \"" << this->m_log_pool_name << "\": "
          << rs << dendl;
  }
}

template <typename I>
void ReplicatedWriteLog<I>::initialize_pool(Context *on_finish, pwl::DeferredContexts &later) {
  CephContext *cct = m_image_ctx.cct;
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  if (access(this->m_log_pool_name.c_str(), F_OK) != 0) {
    if ((m_log_pool = ReplicatedDataPool::create_cache_pool(
	    cct, this->m_log_pool_name.c_str(),
            this->m_log_pool_config_size, 1)) == NULL) {
      lderr(cct) << "failed to create pool (" << this->m_log_pool_name << ")" << dendl;
      m_cache_state->present = false;
      m_cache_state->clean = true;
      m_cache_state->empty = true;
      /* TODO: filter/replace errnos that are meaningless to the caller */
      on_finish->complete(-errno);
      return;
    } 
    m_cache_state->present = true;
    m_cache_state->clean = true;
    m_cache_state->empty = true;

    /* new pool, calculate and store metadata */
    size_t effective_pool_size = (size_t)(this->m_log_pool_config_size * USABLE_SIZE);
    size_t small_write_size = MIN_WRITE_ALLOC_SIZE + BLOCK_ALLOC_OVERHEAD_BYTES + sizeof(struct WriteLogPmemEntry);
    uint64_t num_small_writes = (uint64_t)(effective_pool_size / small_write_size);
    if (num_small_writes > MAX_LOG_ENTRIES) {
      num_small_writes = MAX_LOG_ENTRIES;
    }
    if (num_small_writes <= 2) {
      lderr(cct) << "num_small_writes needs to > 2" << dendl;
      on_finish->complete(-EINVAL);
      return;
    } 
    this->m_log_pool_actual_size = this->m_log_pool_config_size;
    this->m_bytes_allocated_cap = effective_pool_size;
    /* Log ring empty */
    m_first_free_entry = 0;
    m_first_valid_entry = 0;

    WriteLogPoolRoot pool_root;

    pool_root.layout_version = RWL_POOL_VERSION;
    pool_root.log_entries_pos = sizeof(pool_root) * 2; // start of WriteLogPmemEntry 
    pool_root.pool_size = this->m_log_pool_actual_size;
    pool_root.flushed_sync_gen = this->m_flushed_sync_gen;
    pool_root.block_size = MIN_WRITE_ALLOC_SIZE;
    pool_root.num_log_entries = num_small_writes;
    pool_root.first_free_entry = this->m_first_free_entry;
    pool_root.first_valid_entry = this->m_first_valid_entry;

    int r = m_log_pool->init_root(pool_root);
    if (r < 0) {
      this->m_total_log_entries = 0;
      this->m_free_log_entries = 0;
      lderr(cct) << "failed to initialize pool (" << this->m_log_pool_name << ")" << dendl;
      on_finish->complete(r);
      return;
    }

    this->m_total_log_entries = pool_root.num_log_entries;
    this->m_free_log_entries = pool_root.num_log_entries - 1; // leave one free
  } else {
    m_cache_state->present = true;
    /* Open existing pool */
    if ((m_log_pool = ReplicatedDataPool::open_cache_pool(
	    cct, this->m_log_pool_name.c_str(), {})) == NULL) {
      lderr(cct) << "failed to open pool (" << this->m_log_pool_name << "): " << dendl;
      on_finish->complete(-errno);
      return;
    }

    WriteLogPoolRoot* pool_root = m_log_pool->get_root();
    if (pool_root->layout_version != RWL_POOL_VERSION) {
      lderr(cct) << "Pool layout version is " << pool_root->layout_version
                 << " expected " << RWL_POOL_VERSION << dendl;
      on_finish->complete(-EINVAL);
      return;
    }

    if (pool_root->block_size != MIN_WRITE_ALLOC_SIZE) {
      lderr(cct) << "Pool block size is " << pool_root->block_size
                 << " expected " << MIN_WRITE_ALLOC_SIZE << dendl;
      on_finish->complete(-EINVAL);
      return;
    }

    this->m_log_pool_actual_size = pool_root->pool_size;
    this->m_flushed_sync_gen = pool_root->flushed_sync_gen;
    this->m_total_log_entries = pool_root->num_log_entries;
    this->m_first_free_entry = pool_root->first_free_entry;
    this->m_first_valid_entry = pool_root->first_valid_entry;

    if (this->m_first_free_entry < this->m_first_valid_entry) {
      /* Valid entries wrap around the end of the ring, so first_free is lower
       * than first_valid.  If first_valid was == first_free+1, the entry at
       * first_free would be empty. The last entry is never used, so in
       * that case there would be zero free log entries. */
     this->m_free_log_entries = this->m_total_log_entries - (m_first_valid_entry - m_first_free_entry) -1;
    } else {
      /* first_valid is <= first_free. If they are == we have zero valid log
       * entries, and n-1 free log entries */
      this->m_free_log_entries = this->m_total_log_entries - (m_first_free_entry - m_first_valid_entry) -1;
    } 
    size_t effective_pool_size = (size_t)(this->m_log_pool_config_size * USABLE_SIZE);
    this->m_bytes_allocated_cap = effective_pool_size;
    load_existing_entries(later);
    m_cache_state->clean = this->m_dirty_log_entries.empty();
    m_cache_state->empty = m_log_entries.empty();
  }
}

/*
 * Loads the log entries from an existing log.
 *
 * Creates the in-memory structures to represent the state of the
 * re-opened log.
 *
 * Finds the last appended sync point, and any sync points referred to
 * in log entries, but missing from the log. These missing sync points
 * are created and scheduled for append. Some rudimentary consistency
 * checking is done.
 *    
 * Rebuilds the m_blocks_to_log_entries map, to make log entries
 * readable.
 *  
 * Places all writes on the dirty entries list, which causes them all
 * to be flushed.
 *  
 */

template <typename I>
void ReplicatedWriteLog<I>::load_existing_entries(DeferredContexts &later) {
  WriteLogPmemEntry *pmem_log_entries = m_log_pool->get_log_entries();
  uint64_t entry_index = m_first_valid_entry;
  /* The map below allows us to find sync point log entries by sync
   * gen number, which is necessary so write entries can be linked to
   * their sync points. */
  std::map<uint64_t, std::shared_ptr<SyncPointLogEntry>> sync_point_entries;
  /* The map below tracks sync points referred to in writes but not
   * appearing in the sync_point_entries map.  We'll use this to
   * determine which sync points are missing and need to be
   * created. */
  std::map<uint64_t, bool> missing_sync_points;

  /*
   * Read the existing log entries. Construct an in-memory log entry
   * object of the appropriate type for each. Add these to the global
   * log entries list.
   *
   * Write entries will not link to their sync points yet. We'll do
   * that in the next pass. Here we'll accumulate a map of sync point
   * gen numbers that are referred to in writes but do not appearing in
   * the log.
   */
  while (entry_index != m_first_free_entry) {
    WriteLogPmemEntry *pmem_entry = &pmem_log_entries[entry_index];
    std::shared_ptr<GenericLogEntry> log_entry = nullptr;
    ceph_assert(pmem_entry->entry_index == entry_index);

    this->update_entries(log_entry, pmem_entry, missing_sync_points,
        sync_point_entries, entry_index);

    log_entry->ram_entry = *pmem_entry;
    log_entry->pmem_entry = pmem_entry;

    log_entry->log_entry_index = entry_index;
    log_entry->completed = true;

    m_log_entries.push_back(log_entry);
    
    entry_index = (entry_index + 1) % this->m_total_log_entries;
  }

  this->update_sync_points(missing_sync_points, sync_point_entries, later);
}

template <typename I>
void ReplicatedWriteLog<I>::write_data_to_buffer(std::shared_ptr<WriteLogEntry> ws_entry,
    WriteLogPmemEntry *pmem_entry) {
  ws_entry->pmem_buffer = m_log_pool->get_data_buffer(pmem_entry->write_data_pos);
} 

/**
 * Retire up to MAX_ALLOC_PER_TRANSACTION of the oldest log entries
 * that are eligible to be retired. Returns true if anything was
 * retired.
 */
template <typename I>
bool ReplicatedWriteLog<I>::retire_entries(const unsigned long int frees_per_tx) {
  CephContext *cct = m_image_ctx.cct;
  GenericLogEntriesVector retiring_entries;
  uint32_t initial_first_valid_entry;
  uint32_t first_valid_entry;

  std::lock_guard retire_locker(this->m_log_retire_lock);
  ldout(cct, 20) << "Look for entries to retire" << dendl;
  {
    /* Entry readers can't be added while we hold m_entry_reader_lock */
    RWLock::WLocker entry_reader_locker(this->m_entry_reader_lock);
    std::lock_guard locker(m_lock);
    initial_first_valid_entry = this->m_first_valid_entry;
    first_valid_entry = this->m_first_valid_entry;
    auto entry = m_log_entries.front();
    while (!m_log_entries.empty() &&
           retiring_entries.size() < frees_per_tx &&
           this->can_retire_entry(entry)) {
      if (entry->log_entry_index != first_valid_entry) {
        lderr(cct) << "Retiring entry index (" << entry->log_entry_index
                   << ") and first valid log entry index (" << first_valid_entry
                   << ") must be ==." << dendl;
      }
      ceph_assert(entry->log_entry_index == first_valid_entry);
      first_valid_entry = (first_valid_entry + 1) % this->m_total_log_entries;
      m_log_entries.pop_front();
      retiring_entries.push_back(entry);
      /* Remove entry from map so there will be no more readers */
      if ((entry->write_bytes() > 0) || (entry->bytes_dirty() > 0)) {
        auto gen_write_entry = static_pointer_cast<GenericWriteLogEntry>(entry);
        if (gen_write_entry) {
          this->m_blocks_to_log_entries.remove_log_entry(gen_write_entry);
        }
      }
      entry = m_log_entries.front();
    }
  }

  if (retiring_entries.size()) {
    ldout(cct, 20) << "Retiring " << retiring_entries.size() << " entries" << dendl;

    utime_t tx_start;
    /* Advance first valid entry and release buffers */
    {
      uint64_t flushed_sync_gen;
      std::lock_guard append_locker(this->m_log_append_lock);
      {
        std::lock_guard locker(m_lock);
        flushed_sync_gen = this->m_flushed_sync_gen;
      }

      tx_start = ceph_clock_now();

      m_log_pool->update_flushed_sync_gen(flushed_sync_gen);
      m_log_pool->update_first_valid_entry(first_valid_entry);

      for (auto &entry: retiring_entries) {
        if (entry->write_bytes()) {
          m_log_pool->release(entry->data_offset(), entry->write_bytes());
        }
      }
    }
    utime_t tx_end = ceph_clock_now();
    m_perfcounter->tinc(l_librbd_pwl_retire_tx_t, tx_end - tx_start);
    m_perfcounter->hinc(l_librbd_pwl_retire_tx_t_hist, utime_t(tx_end - tx_start).to_nsec(),
        retiring_entries.size());

    /* Update runtime copy of first_valid, and free entries counts */
    {
      std::lock_guard locker(m_lock);

      ceph_assert(this->m_first_valid_entry == initial_first_valid_entry);
      this->m_first_valid_entry = first_valid_entry;
      this->m_free_log_entries += retiring_entries.size();
      for (auto &entry: retiring_entries) {
        if (entry->write_bytes()) {
          ceph_assert(this->m_bytes_cached >= entry->write_bytes());
          this->m_bytes_cached -= entry->write_bytes();
          uint64_t entry_allocation_size = entry->write_bytes();
          if (entry_allocation_size < MIN_WRITE_ALLOC_SIZE) {
            entry_allocation_size = MIN_WRITE_ALLOC_SIZE;
          }
          ceph_assert(this->m_bytes_allocated >= entry_allocation_size);
          this->m_bytes_allocated -= entry_allocation_size;
        }
      } 
      this->m_alloc_failed_since_retire = false;
      this->wake_up();
    }
  } else {
    ldout(cct, 20) << "Nothing to retire" << dendl;
    return false;
  }
  return true;
}

template <typename I>
Context* ReplicatedWriteLog<I>::construct_flush_entry_ctx(
    std::shared_ptr<GenericLogEntry> log_entry) {
  bool invalidating = this->m_invalidating; // snapshot so we behave consistently
  Context *ctx = this->construct_flush_entry(log_entry, invalidating);
  
  if (invalidating) {
    return ctx;
  }
  return new LambdaContext(
    [this, log_entry, ctx](int r) {
      m_image_ctx.op_work_queue->queue(new LambdaContext(
        [this, log_entry, ctx](int r) {
          ldout(m_image_ctx.cct, 15) << "flushing:" << log_entry
                                     << " " << *log_entry << dendl;
          log_entry->writeback(this->m_image_writeback, ctx);
        }), 0);
    }); 
}

const unsigned long int ops_flushed_together = 4;
/*
 * Performs the pmem buffer flush on all scheduled ops, then schedules
 * the log event append operation for all of them.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_then_append_scheduled_ops(void)
{
  GenericLogOperations ops;
  bool ops_remain = false;
  ldout(m_image_ctx.cct, 20) << dendl;
  do {
    {
      ops.clear();
      std::lock_guard locker(m_lock);
      if (m_ops_to_flush.size()) {
        auto last_in_batch = m_ops_to_flush.begin();
        unsigned int ops_to_flush = m_ops_to_flush.size();
        if (ops_to_flush > ops_flushed_together) {
          ops_to_flush = ops_flushed_together;
        }
        ldout(m_image_ctx.cct, 20) << "should flush " << ops_to_flush << dendl;
        std::advance(last_in_batch, ops_to_flush);
        ops.splice(ops.end(), m_ops_to_flush, m_ops_to_flush.begin(), last_in_batch);
        ops_remain = !m_ops_to_flush.empty();
        ldout(m_image_ctx.cct, 20) << "flushing " << ops.size() << ", "
                                   << m_ops_to_flush.size() << " remain" << dendl;
      } else {
        ops_remain = false;
      }
    } 
    if (ops_remain) {
      enlist_op_flusher();
    }

    /* Ops subsequently scheduled for flush may finish before these,
     * which is fine. We're unconcerned with completion order until we
     * get to the log message append step. */
    if (ops.size()) {
      flush_pmem_buffer(ops);
      schedule_append_ops(ops);
    } 
  } while (ops_remain);
  append_scheduled_ops();
}

/*
 * Performs the log event append operation for all of the scheduled
 * events.
 */
template <typename I>
void ReplicatedWriteLog<I>::append_scheduled_ops(void) {
  GenericLogOperations ops;
  int append_result = 0;
  bool ops_remain = false;
  bool appending = false; /* true if we set m_appending */
  ldout(m_image_ctx.cct, 20) << dendl;
  do {
    ops.clear();
    this->append_scheduled(ops, ops_remain, appending, true);
    
    if (ops.size()) {
      std::lock_guard locker(this->m_log_append_lock);
      alloc_op_log_entries(ops);
      append_result = append_op_log_entries(ops);
    } 
    
    int num_ops = ops.size();
    if (num_ops) {
      /* New entries may be flushable. Completion will wake up flusher. */
      this->complete_op_log_entries(std::move(ops), append_result);
    } 
  } while (ops_remain);
}

template <typename I>
void ReplicatedWriteLog<I>::enlist_op_flusher()
{
  this->m_async_flush_ops++;
  this->m_async_op_tracker.start_op();
  Context *flush_ctx = new LambdaContext([this](int r) {
      flush_then_append_scheduled_ops();
      this->m_async_flush_ops--;
      this->m_async_op_tracker.finish_op();
    });
  this->m_work_queue.queue(flush_ctx);
}

template <typename I>
void ReplicatedWriteLog<I>::setup_schedule_append(
    pwl::GenericLogOperationsVector &ops, bool do_early_flush) {
  if (do_early_flush) {                           
    /* This caller is waiting for persist, so we'll use their thread to
     * expedite it */
    flush_pmem_buffer(ops);
    this->schedule_append(ops);
  } else {
    /* This is probably not still the caller's thread, so do the payload
     * flushing/replicating later. */
    schedule_flush_and_append(ops);
  } 
}

/*
 * Takes custody of ops. They'll all get their log entries appended,
 * and have their on_write_persist contexts completed once they and
 * all prior log entries are persisted everywhere.
 */
template <typename I>
void ReplicatedWriteLog<I>::schedule_append_ops(GenericLogOperations &ops)
{
  bool need_finisher;
  GenericLogOperationsVector appending;
  
  std::copy(std::begin(ops), std::end(ops), std::back_inserter(appending));
  {
    std::lock_guard locker(m_lock);
  
    need_finisher = this->m_ops_to_append.empty() && !this->m_appending;
    this->m_ops_to_append.splice(this->m_ops_to_append.end(), ops);
  } 
  
  if (need_finisher) {
    this->enlist_op_appender();
  }
  
  for (auto &op : appending) {
    op->appending();
  }
}

/*
 * Takes custody of ops. They'll all get their pmem blocks flushed,
 * then get their log entries appended.
 */
template <typename I>
void ReplicatedWriteLog<I>::schedule_flush_and_append(GenericLogOperationsVector &ops)
{
  GenericLogOperations to_flush(ops.begin(), ops.end());
  bool need_finisher;
  ldout(m_image_ctx.cct, 20) << dendl;
  {
    std::lock_guard locker(m_lock);
    
    need_finisher = m_ops_to_flush.empty();
    m_ops_to_flush.splice(m_ops_to_flush.end(), to_flush);
  } 
  
  if (need_finisher) {
    enlist_op_flusher();
  } 
}

template <typename I>
void ReplicatedWriteLog<I>::process_work() {
  CephContext *cct = m_image_ctx.cct;
  int max_iterations = 4;
  bool wake_up_requested = false;
  uint64_t aggressive_high_water_bytes = this->m_bytes_allocated_cap * AGGRESSIVE_RETIRE_HIGH_WATER;
  uint64_t high_water_bytes = this->m_bytes_allocated_cap * RETIRE_HIGH_WATER;
  uint64_t low_water_bytes = this->m_bytes_allocated_cap * RETIRE_LOW_WATER;
  uint64_t aggressive_high_water_entries = this->m_total_log_entries * AGGRESSIVE_RETIRE_HIGH_WATER;
  uint64_t high_water_entries = this->m_total_log_entries * RETIRE_HIGH_WATER;
  uint64_t low_water_entries = this->m_total_log_entries * RETIRE_LOW_WATER;

  ldout(cct, 20) << dendl;

  do {
    {
      std::lock_guard locker(m_lock);
      this->m_wake_up_requested = false;
    }
    if (this->m_alloc_failed_since_retire || this->m_invalidating ||
        this->m_bytes_allocated > high_water_bytes ||
        (m_log_entries.size() > high_water_entries)) {
      int retired = 0;
      utime_t started = ceph_clock_now();
      ldout(m_image_ctx.cct, 10) << "alloc_fail=" << this->m_alloc_failed_since_retire
                                 << ", allocated > high_water="
                                 << (this->m_bytes_allocated > high_water_bytes)
                                 << ", allocated_entries > high_water="
                                 << (m_log_entries.size() > high_water_entries)
                                 << dendl;
      while (this->m_alloc_failed_since_retire || this->m_invalidating ||
            (this->m_bytes_allocated > high_water_bytes) ||
            (m_log_entries.size() > high_water_entries) ||
            (((this->m_bytes_allocated > low_water_bytes) ||
              (m_log_entries.size() > low_water_entries)) &&
            (utime_t(ceph_clock_now() - started).to_msec() < RETIRE_BATCH_TIME_LIMIT_MS))) {
        if (!retire_entries((this->m_shutting_down || this->m_invalidating ||
           (this->m_bytes_allocated > aggressive_high_water_bytes) ||
           (m_log_entries.size() > aggressive_high_water_entries))
            ? MAX_ALLOC_PER_TRANSACTION
            : MAX_FREE_PER_TRANSACTION)) {
          break;
        }
        retired++;
        this->dispatch_deferred_writes();
        this->process_writeback_dirty_entries();
      }
      ldout(m_image_ctx.cct, 10) << "Retired " << retired << " times" << dendl;
    }
    this->dispatch_deferred_writes();
    this->process_writeback_dirty_entries();

    {
      std::lock_guard locker(m_lock);
      wake_up_requested = this->m_wake_up_requested;
    }
  } while (wake_up_requested && --max_iterations > 0);

  {
    std::lock_guard locker(m_lock);
    this->m_wake_up_scheduled = false;
    /* Reschedule if it's still requested */
    if (this->m_wake_up_requested) {
      this->wake_up();
    } 
  }
}

/*
 * Flush the pmem regions for the data blocks of a set of operations
 *
 * V is expected to be GenericLogOperations<I>, or GenericLogOperationsVector<I>
 */
template <typename I>
template <typename V>
void ReplicatedWriteLog<I>::flush_pmem_buffer(V& ops)
{
  io::Extents extents;
  for (auto &operation : ops) {
    operation->flush_pmem_buf_to_cache(extents);
  } 
  
  m_log_pool->flush(extents); // flush and drain
  
  utime_t now = ceph_clock_now();
  for (auto &operation : ops) {
    if (operation->reserved_allocated()) {
      operation->buf_persist_comp_time = now;
    } else {
      ldout(m_image_ctx.cct, 20) << "skipping non-write op: " << *operation << dendl;
    } 
  } 
}

/**
 * Update/persist the last flushed sync point in the log
 */
template <typename I>
void ReplicatedWriteLog<I>::persist_last_flushed_sync_gen()
{
  uint64_t flushed_sync_gen;

  std::lock_guard append_locker(this->m_log_append_lock);
  {
    std::lock_guard locker(m_lock);
    flushed_sync_gen = this->m_flushed_sync_gen;
  }

  WriteLogPoolRoot* pool_root = m_log_pool->get_root();
  if (pool_root->flushed_sync_gen < flushed_sync_gen) {
    ldout(m_image_ctx.cct, 15) << "flushed_sync_gen in log updated from "
                               << pool_root->flushed_sync_gen << " to "
                               << flushed_sync_gen << dendl;
    m_log_pool->update_flushed_sync_gen(flushed_sync_gen);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::reserve_pmem(C_BlockIORequestT *req,
                                         bool &alloc_succeeds, bool &no_space) {
  std::vector<WriteBufferAllocation>& buffers = req->get_resources_buffers();
  for (auto &buffer : buffers) {
    utime_t before_reserve = ceph_clock_now();
    buffer.buffer_off = m_log_pool->allocate(buffer.allocation_size);
    buffer.allocation_lat = ceph_clock_now() - before_reserve;
    if (buffer.buffer_off <= 0) {
      if (!req->has_io_waited_for_buffers()) {
        req->set_io_waited_for_entries(true);
      } 
      ldout(m_image_ctx.cct, 5) << "can't allocate all data buffers: "
                                << *req << dendl;
      alloc_succeeds = false;   
      no_space = true; /* Entries need to be retired */
      break;
    } else {
      buffer.allocated = true;
    } 
    ldout(m_image_ctx.cct, 20) << "Allocated " << buffer.buffer_off
                               << ", size=" << buffer.allocation_size << dendl;
  }                            
}

template <typename I>
void ReplicatedWriteLog<I>::copy_pmem(C_BlockIORequestT *req) {
  req->copy_pmem();
}

template <typename I>
bool ReplicatedWriteLog<I>::alloc_resources(C_BlockIORequestT *req) {
  bool alloc_succeeds = true;
  uint64_t bytes_allocated = 0;
  uint64_t bytes_cached = 0;
  uint64_t bytes_dirtied = 0;
  uint64_t num_lanes = 0;
  uint64_t num_unpublished_reserves = 0;
  uint64_t num_log_entries = 0;
  
  ldout(m_image_ctx.cct, 20) << dendl;
  // Setup buffer, and get all the number of required resources
  req->setup_buffer_resources(bytes_cached, bytes_dirtied, bytes_allocated,
                              num_lanes, num_log_entries, num_unpublished_reserves);
                              
  alloc_succeeds = this->check_allocation(req, bytes_cached, bytes_dirtied, bytes_allocated,
                              num_lanes, num_log_entries, num_unpublished_reserves,
                              this->m_bytes_allocated_cap);
                              
  std::vector<WriteBufferAllocation>& buffers = req->get_resources_buffers();
  if (!alloc_succeeds) {
    /* On alloc failure, free any buffers we did allocate */
    for (auto &buffer : buffers) {
      if (buffer.allocated) {
	m_log_pool->release(buffer.buffer_off, buffer.allocation_size);
      }
    } 
  } 
  
  req->set_allocated(alloc_succeeds);
  return alloc_succeeds;
}

} // namespace pwl
} // namespace cache
} // namespace librbd

template class librbd::cache::pwl::ReplicatedWriteLog<librbd::ImageCtx>;
