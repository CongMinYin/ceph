// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <stddef.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "alloc/Allocator.h"
#include "include/Context.h"
#include "include/ceph_assert.h"
#include "common/dout.h"
#include "common/environment.h"
#include "common/errno.h"
#include "common/debug.h"
#include "libpmem.h"
#include "librbd/cache/pwl/ReplicatedDataPool.h"

#define dout_context cct
#undef dout_subsys
#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::ReplicatedDataPool: " << " " \
                           <<  __func__ << ": "


namespace librbd {
namespace cache {
namespace pwl {

LocalDataCopy::LocalDataCopy(CephContext* cct, string path,
                             int fd, char *addr, uint64_t size)
  : cct(cct), m_path(path), m_fd(fd), m_addr(addr), m_size(size) {
}

LocalDataCopy* LocalDataCopy::create(CephContext* cct, std::string pool_path, uint64_t &size) {
  ldout(cct, 20) << dendl;

  int mode = S_IWRITE | S_IREAD;
  int flags = O_RDWR | O_CREAT | O_EXCL;
  int fd;

  if ((fd = ::open(pool_path.c_str(), flags, mode)) < 0) {
    lderr(cct) << ": failed to create cache file " << dendl;
    return nullptr;
  }

  if ((::posix_fallocate(fd, 0, (off_t)size)) == 0) {
    if (::flock(fd, LOCK_EX | LOCK_NB) >= 0) {
      // create local pool
      size_t map_len;
      char* addr = (char *)pmem_map_file(pool_path.c_str(), 0, PMEM_FILE_EXCL, O_RDWR, &map_len, NULL);
      if (addr != nullptr) {
        ldout(cct, 10) << "Created local cache file successfully." << dendl;
	LocalDataCopy* local = new LocalDataCopy(cct, pool_path, fd, addr, map_len);
	size = map_len;
	return local;
      }
    } else {
      lderr(cct) << ": failed to lock cache file " << dendl;
    }
  } else {
    lderr(cct) << ": failed to allocate cache file " << dendl;
  }

  // error happens
  if (fd != -1) {
    ::close(fd);
  }
  ::unlink(pool_path.c_str());
  return nullptr;
}

LocalDataCopy* LocalDataCopy::open(CephContext* cct, std::string pool_path, uint64_t& size) {
  ldout(cct, 20) << dendl;

  int mode = S_IWRITE | S_IREAD;
  int flags = O_RDWR | O_EXCL;
  int fd;

  if ((fd = ::open(pool_path.c_str(), flags, mode)) < 0) {
    lderr(cct) << ": failed to open cache file " << dendl;
    return nullptr;
  }

  if (::flock(fd, LOCK_EX | LOCK_NB) >= 0) {
    size_t map_len;
    char* addr = (char *)pmem_map_file(pool_path.c_str(), 0, PMEM_FILE_EXCL, O_RDWR, &map_len, NULL);
    if (addr != nullptr) {
      ldout(cct, 10) << "Created local cache file successfully." << dendl;
      LocalDataCopy* local = new LocalDataCopy(cct, pool_path, fd, addr, map_len);
      size = map_len;
      return local;
    }
  } else {
    lderr(cct) << ": failed to lock cache file " << dendl;
  }

  // error happens
  if (fd != -1) {
    ::close(fd);
  }
  ::unlink(pool_path.c_str());
  return nullptr;
}

ReplicatedDataPool* ReplicatedDataPool::create_cache_pool(
    CephContext* cct, std::string pool_path, uint64_t size, int copies) {

  ldout(cct, 20) << dendl;
  // librados::RadosClient::mon_command to get replica list
  /*
   * Initialize local pool at first
   * Initialize rpma_peer if replicas are needed
   * register local memory
   * messenger to initialize replicas
   * connect to replica
   * Repeat the last two steps until copies are satisfied
   */

  ceph_assert(copies >= 1);
  LocalDataCopy* primary = LocalDataCopy::create(cct, pool_path, size);
  if (!primary) {
    lderr(cct) << " failed to create the primary copy" << dendl;
    return nullptr;
  }

  vector<RemoteDataCopy*> replicas;
  if (copies > 1) {
    // TODO: add remote copies.
    //
  }

  ReplicatedDataPool* pool = new ReplicatedDataPool(cct, primary, replicas, size);
  return pool;
}

ReplicatedDataPool* ReplicatedDataPool::open_cache_pool(
  CephContext* cct, std::string pool_path, std::vector<std::string> replicas_config) {

  ldout(cct, 20) << dendl;
  uint64_t size;
  LocalDataCopy* primary = LocalDataCopy::open(cct, pool_path, size);
  if (!primary) {
    lderr(cct) << " failed to open the primary copy" << dendl;
    return nullptr;
  }

  vector<RemoteDataCopy*> replicas;
  for (std::string& item : replicas_config) {
    dout(20) << item << dendl;
    // TODO: Open remote copies
  }

  ReplicatedDataPool* pool = new ReplicatedDataPool(cct, primary, replicas, size);
  return pool;
}

#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::ReplicatedDataPool: " << this << " " \
                           <<  __func__ << ": "

void LocalDataCopy::init() {
}

int LocalDataCopy::read(char* data, uint64_t off, uint64_t len) {
  dout(20) << " " << off << "~" << len  << dendl;

  memcpy(data, m_addr + off, len);
  return 0;
}

int LocalDataCopy::write(const char* data, uint64_t off, uint64_t len) {
  dout(20) << " " << off << "~" << len  << dendl;

  pmem_memcpy(m_addr + off, data, len, 0);
  return 0;
}

int LocalDataCopy::persist_atomic(uint64_t off, uint64_t data) {
  dout(20) << " " << off << dendl;

  pmem_memcpy_persist(m_addr + off, (char*)&data, sizeof(uint64_t));
  
  return 0;
}

char* LocalDataCopy::get_buffer(uint64_t off) {
  dout(20) << dendl;
  return (char*)(m_addr + off);
}

void LocalDataCopy::flush(uint64_t off, uint64_t len) {
  dout(20) << " " << off << "~" << len  << dendl;

  pmem_persist((m_addr + off), len);
  pmem_drain();
}

void LocalDataCopy::flush(io::Extents &ops) {
  dout(20) << dendl;

  for (auto& item : ops) {
    dout(20) << " " << item.first << "~" << item.second  << dendl;
    pmem_persist(m_addr + item.first, item.second);
  }
  pmem_drain();
}

int LocalDataCopy::close(bool deleted) {
  dout(20) << dendl;

  ceph_assert(m_addr != NULL);
  pmem_unmap(m_addr, m_size);
  ceph_assert(m_fd >= 0);
  VOID_TEMP_FAILURE_RETRY(::close(m_fd));
  m_fd = -1;
  int r = ::remove(m_path.c_str());
  return r;
}


void RemoteDataCopy::init() {
}

int RemoteDataCopy::write(io::Extents &ops, Context *on_finish) {
  return 0;
}

void RemoteDataCopy::flush(Context *on_finish) {
}

// ReplicatedDataPool
int ReplicatedDataPool::addDataCopy() {
  return 0;
}

int ReplicatedDataPool::removeDataCopy() {
  return 0;
}

int ReplicatedDataPool::read(ceph::bufferlist* pbl, uint64_t off, uint64_t len) {
  dout(20) << " " << off << "~" << len  << dendl;
  // ceph_assert(is_valid_io(off, len));

  bufferptr p = buffer::create_small_page_aligned(len);
  m_primary->read(p.c_str(), off, len);

  pbl->clear();
  pbl->push_back(std::move(p));

  dout(40) << "data: ";
  pbl->hexdump(*_dout);
  *_dout << dendl;

  return len;
}

int ReplicatedDataPool::write(ceph::bufferlist& bl, uint64_t off, uint64_t len) {
  dout(20) << " " << off << "~" << len  << dendl;

  // Write to local pmem at first, persist to remote copies when flushing
  bufferlist::iterator p = bl.begin();
  uint64_t off1 = off;
  while (len) {
    const char *data;
    uint32_t l = p.get_ptr_and_advance(len, &data);
    m_primary->write(data, off1, l);

    len -= l;
    off1 += l;
  }

  return 0;
}

void ReplicatedDataPool::flush(uint64_t off, uint64_t len) {
  dout(20) << " " << off << "~" << len  << dendl;

  m_primary->flush(off, len);
}

void ReplicatedDataPool::flush(io::Extents& ops) {
  dout(20) << dendl;

  m_primary->flush(ops);
}

void ReplicatedDataPool::flush_log_entries(uint64_t index, int number) {
  dout(20) << " flush " << number << " log entries from index " << index << dendl;

  uint64_t off = m_pool_root.log_entries_pos + index * sizeof(WriteLogPmemEntry);
  uint64_t len = sizeof(WriteLogPmemEntry) * number;
  flush(off, len);
}

void ReplicatedDataPool::update_first_free_entry(uint64_t first_free_entry) {
  dout(20) << " first_free_entry=" << first_free_entry << dendl;
  // find out the off of first_free_entry in m_pool_root
  size_t off = offsetof(struct WriteLogPoolRoot, first_free_entry);
  m_primary->persist_atomic(off, first_free_entry);
}

void ReplicatedDataPool::update_flushed_sync_gen(uint64_t flushed_sync_gen) {
  dout(20) << " flushed_sync_gen=" << flushed_sync_gen << dendl;
  size_t off = offsetof(struct WriteLogPoolRoot, flushed_sync_gen);
  m_primary->persist_atomic(off, flushed_sync_gen);
}

void ReplicatedDataPool::update_first_valid_entry(uint64_t first_valid_entry) {
  dout(20) << " first_valid_entry=" << first_valid_entry << dendl;
  size_t off = offsetof(struct WriteLogPoolRoot, first_valid_entry);
  m_primary->persist_atomic(off, first_valid_entry);
}

WriteLogPoolRoot* ReplicatedDataPool::get_root() {
  dout(20) << dendl;

  return (WriteLogPoolRoot*)m_primary->get_buffer(0);
}

int ReplicatedDataPool::init_root(WriteLogPoolRoot &pool_root) {
  dout(20) << dendl;

  m_pool_root = pool_root;

  char* data = (char*)(&m_pool_root);
  m_primary->write(data, 0, sizeof(WriteLogPoolRoot));
  m_primary->flush(0, sizeof(WriteLogPoolRoot));
  // write to remote copies

  // initialize the allocator
  uint64_t off = m_pool_root.log_entries_pos * sizeof(WriteLogPmemEntry);
  uint64_t len = m_size - off;
  m_alloc->init_add_free(off, len);
  return 0;
}

int ReplicatedDataPool::write_log_entry(uint64_t index, WriteLogPmemEntry &ram_entry) {
  dout(20) << " write log entries at index " << index << dendl;

  uint64_t off = m_pool_root.log_entries_pos + index * sizeof(WriteLogPmemEntry);
  uint64_t len = sizeof(WriteLogPmemEntry);
  return m_primary->write((char*)(&ram_entry), off, len);
}

WriteLogPmemEntry* ReplicatedDataPool::get_log_entries() {
  dout(20) << dendl;

  return (WriteLogPmemEntry*)m_primary->get_buffer(m_pool_root.log_entries_pos);
}

uint8_t* ReplicatedDataPool::get_data_buffer(uint64_t off) {
  dout(20) << " off=" << off << dendl;

  return (uint8_t*)m_primary->get_buffer(off);
}

int64_t ReplicatedDataPool::allocate(uint64_t len) {
  dout(20) << " len=" << len << dendl;

  PExtentVector exts;
  // Allocate sequential space
  uint64_t aligned_size = (((len) + m_alloc_size - 1) & ~(m_alloc_size - 1));
  int64_t alloc_len = m_alloc->allocate(aligned_size, aligned_size, 0, 0, &exts);

  if (alloc_len <= 0) {
    return alloc_len;
  }

  ceph_assert(exts.size() == 1);
  return exts[0].offset;
}

void ReplicatedDataPool::release(uint64_t off, uint64_t len) {
  dout(20) << " off=" << off << " len=" << len << dendl;
  uint64_t aligned_size = (((len) + m_alloc_size - 1) & ~(m_alloc_size - 1));

  interval_set<uint64_t> release_set;
  release_set.insert(off, aligned_size);

  m_alloc->release(release_set);
}

void ReplicatedDataPool::set_allocated(uint64_t off, uint64_t len) {
  dout(20) << " off=" << off << " len=" << len << dendl;
  uint64_t aligned_size = (((len) + m_alloc_size - 1) & ~(m_alloc_size - 1));
  m_alloc->init_rm_free(off, aligned_size);
}

int ReplicatedDataPool::close(bool deleted) {
  dout(20) << dendl;

  int r = 0;
  if (m_primary) {
    r = m_primary->close(deleted);
  }

  for (RemoteDataCopy* item : m_replicas) {
    if (item) {
      int rs = item->close(deleted);
      r = (r < 0) ? r : rs;
    }
  }
  return r;
}

ReplicatedDataPool::ReplicatedDataPool(CephContext* cct,
                                       LocalDataCopy* primary,
				       vector<RemoteDataCopy*>& remote,
				       uint64_t size)
  : cct(cct), m_size(size), m_primary(primary) {
  m_replicas.insert(m_replicas.end(), remote.begin(), remote.end());

  m_alloc = Allocator::create(cct, "stupid",
                              m_size,
                              m_alloc_size, "block");

  ceph_assert(m_alloc);
}

ReplicatedDataPool::~ReplicatedDataPool() {
  ceph_assert(m_alloc);
  ceph_assert(m_primary);
  delete m_alloc;
  delete m_primary;
  
  for (auto replica : m_replicas) {
    delete replica;
  }
}

} // namespace pwl
} // namespace cache
} // namespace librbd

