// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PWL_REPLICATED_DATA_POOL_H
#define CEPH_LIBRBD_CACHE_PWL_REPLICATED_DATA_POOL_H

#include "common/ceph_mutex.h"
#include "librbd/Utils.h"
#include "librbd/cache/pwl/Types.h"
#include <atomic>
#include <memory>

class Allocator;

namespace librbd {
namespace cache {
namespace pwl {

class LocalDataCopy {
public:
  CephContext* cct;

  void init();
  int close(bool deleted);

  int read(char *data, uint64_t off, uint64_t len);
  int write(const char *data, uint64_t off, uint64_t len);

  int persist_atomic(uint64_t off, uint64_t data);

  char* get_buffer(uint64_t off);

  void flush(uint64_t off, uint64_t len);
  void flush(io::Extents &ops);

  static LocalDataCopy* create(CephContext* cct, std::string pool_path, uint64_t& size);
  static LocalDataCopy* open(CephContext* cct, std::string pool_path, uint64_t& size);

private:
  LocalDataCopy(CephContext* cct, string path,
                int fd, char* addr, uint64_t size);
  string m_path;
  int m_fd;
  char* m_addr;
  uint64_t m_size;
};


class RemoteDataCopy {
public:
  void init();
  int write(io::Extents &ops, Context *on_finish);
  void flush(Context *on_finish);

  int close(bool deleted) {return 0;}

private:
  // RDMA connection
//  struct rpma_conn* m_conn;
};

class ReplicatedDataPool {
public:
  CephContext* cct;

  ~ReplicatedDataPool();

  int addDataCopy();
  int removeDataCopy();

  // sync interfaces in the first phase
  int read(ceph::bufferlist* pbl, uint64_t off, uint64_t len); // read from local
  int read(char *data, uint64_t off, uint64_t len);
  int write(ceph::bufferlist& bl, uint64_t off, uint64_t len); // write to local, flush to remote during persisting
  int write(char *data, uint64_t off, uint64_t len);

  void flush(uint64_t off, uint64_t len);
  void flush(io::Extents &ops); // flush vector <off, len> and drain

  // root
  void update_first_free_entry(uint64_t first_free_entry);
  void update_flushed_sync_gen(uint64_t flushed_sync_gen);
  void update_first_valid_entry(uint64_t first_valid_entry);

  WriteLogPoolRoot* get_root();
  int init_root(WriteLogPoolRoot &pool_root); // init pool root

  // operations to log entry
  void flush_log_entries(uint64_t index, int number);
  int write_log_entry(uint64_t index, WriteLogPmemEntry &ram_entry);
  WriteLogPmemEntry* get_log_entries(); // return the pointer to the first entry
  uint8_t* get_data_buffer(uint64_t off); // return the pointer of data buffer

  // allocator
  int64_t allocate(uint64_t len); // return off. If can't allocate, return -1.
  void release(uint64_t off, uint64_t len);
  void set_allocated(uint64_t off, uint64_t len); // to rebuild the allocator after starting

  int close(bool deleted);

  // static
  static ReplicatedDataPool* create_cache_pool(
    CephContext* cct, std::string pool_name, uint64_t size, int copies);
  static ReplicatedDataPool* open_cache_pool(
    CephContext* cct, std::string pool_name, std::vector<std::string> replicas);

private:
  uint64_t m_size;
  uint64_t m_alloc_size = 512;

  Allocator *m_alloc = nullptr;

  LocalDataCopy* m_primary;
  std::vector<RemoteDataCopy*> m_replicas;

  ReplicatedDataPool(CephContext* cct, LocalDataCopy* primary,
                     vector<RemoteDataCopy*>& remote,
		     uint64_t size);

  WriteLogPoolRoot m_pool_root;
};


} // namespace pwl
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_PWL_REPLICATED_DATA_POOL_H
