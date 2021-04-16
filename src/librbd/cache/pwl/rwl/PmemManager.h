// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_RWL_PMEMMANAGER_H
#define CEPH_LIBRBD_CACHE_RWL_PMEMMANAGER_H

// 提供pmem设备地址空间，操作接口，先写默认m_is_pmem，后考虑不是pmem
// 基于bitset设计关键管理，顺序分配和回收空间
// 空间的最小单位是MIN_WRITE_ALLOC_SIZE,对应一个bit
// head与tail也需要持久化到pmem中
// entry和bit不是一一对应关系，bit=512B，1一个entry可以对应多个bit
// 可以把空间管理图形移动到上面来
// 默认pmem_map_file返回的地址8字节对齐
// 由于pmem_map_file返回的是NULL，判断的时候直接用了求反而不是与NULL比较
// size_t 有没有位溢出的问题， long unsigned int是8字节

#include <libpmem.h>
//#include <bitset>
#include "librbd/cache/pwl/Types.h"
//using std::bitset;
namespace librbd {
namespace cache {
namespace pwl {
namespace rwl {

struct PmemDev {
 public:
  static PmemDev *pmem_create_dev(const char *path, const uint64_t size,
                                  CephContext *cct);
  static PmemDev *pmem_open_dev(const char *path, CephContext *cct);
  char *create_dev(const char *path, const uint64_t size, int *is_pmem);
  char *open_dev(const char *path, int *is_pmem);
  void close_dev();
  void init_data_bit(uint64_t root_space_size);
  void set_data_bit(uint64_t root_space_size, uint64_t first_valid_bit,
      uint64_t last_valid_entry_start_bit, uint64_t last_valid_entry_size);
  uint64_t alloc(uint64_t size);
  void release_to_here(uint64_t last_retire_entry_bit,
                                uint64_t last_retire_entry_size);
  char *get_head_addr();
  size_t get_mapped_len();
  bool is_pmem();
  PmemDev(const char * path, const uint64_t size, CephContext *cct);
  PmemDev(const char * path, CephContext *cct);
  ~PmemDev();
private:
  const char *m_path;
  size_t m_mapped_len = 0;  // size of the actual mapping, generally, m_mapped_len == m_size
  char *m_head_addr = nullptr; // the head addr of pmem device
  bool m_is_pmem = 0;
  CephContext *m_cct;

  /*    |  root   | entries |                data area                      |
   *    --------------------------------------------------------------------
   *    |root+copy| enrties |      free      |   used         |    free     |
   *    --------------------------------------------------------------------
   *    |         |         |                |                |             |
   *    0           m_first_date_bit  m_first_valid_bit  m_first_free_bit  m_max_bit
   *
   */
  // space management
  uint64_t m_max_bit = 0;        // 实际的空间大小,最大的位下标，分配位图的时候按最大的宏定义
  //std::bitset<MAX_LOG_ENTRIES> m_space;
  //bitset<MAX_LOG_ENTRIES> m_space;  // 默认初始化为0
  // 下面这部分是否跟writelog.cc中的m_first_free_entry，m_first_valid_entry有重叠呢
  uint64_t m_first_date_bit = 0;      // skip root and entry area
  uint64_t m_first_free_bit = 0;      // for alloc, data head +1
  uint64_t m_first_valid_bit = 0;     // for free, data tail
};

} // namespace rwl
} // namespace pwl
} // namespace cache
} // namespace librbd
#endif // CEPH_LIBRBD_CACHE_RWL_PMEMMANAGER_H