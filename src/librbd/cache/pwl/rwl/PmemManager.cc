// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PmemManager.h"
// #include "librbd/cache/pwl/LogEntry.h" // 包含了type.h

// 这句undef是不是必须的，重复define会怎么样，如果是必须的，需要修改其他原有的地方
// 貌似不是必须的，重复define会取最后一个值
#undef dout_subsys
#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::rwl::PmemManager: " \
                           << " " <<  __func__ << ": "
namespace librbd {
namespace cache {
namespace pwl {
namespace rwl {

static inline int size_to_bit(uint64_t size) {
  return size % MIN_WRITE_ALLOC_SIZE ? size / MIN_WRITE_ALLOC_SIZE + 1 :
         size / MIN_WRITE_ALLOC_SIZE;
}

PmemDev::PmemDev(const char * path, const uint64_t size, CephContext *cct)
  : m_path(path), m_cct(cct)
{
  int is_pmem = 0;
  m_head_addr = create_dev(path, size, &is_pmem);
  if (!m_head_addr) {
    lderr(m_cct) << "failed to create pmem device." << dendl;
    return;
  }
  m_is_pmem = is_pmem ? true : false;
  m_max_bit = m_mapped_len / MIN_WRITE_ALLOC_SIZE - 1;  // start from 0
  ldout(m_cct, 5) << "successfully created pmem Device. "
                << " path: " << m_path
                << " is_pmem: " << m_is_pmem
                << " m_mapped_len: " << m_mapped_len
                << " m_head_addr: " << m_head_addr
                << " m_first_date_bit: " << m_first_date_bit
                << " m_first_free_bit: " << m_first_free_bit
                << " m_first_valid_bit: " << m_first_valid_bit
                << " m_max_bit: " << m_max_bit
                << dendl;
}

PmemDev::PmemDev(const char * path, CephContext *cct)
  : m_path(path), m_cct(cct)
{
  int is_pmem = 0;
  m_head_addr = open_dev(path, &is_pmem);
  if (!m_head_addr) {
    lderr(m_cct) << "failed to create pmem device." << dendl;
    return;
  }
  m_is_pmem = is_pmem ? true : false;
  m_max_bit = m_mapped_len / MIN_WRITE_ALLOC_SIZE - 1;  // start from 0
  ldout(m_cct, 5) << "successfully map pmem Device. "
                << " path: " << m_path
                << " is_pmem: " << m_is_pmem
                << " m_mapped_len: " << m_mapped_len
                << " m_head_addr: " << m_head_addr
                << " m_first_date_bit: " << m_first_date_bit
                << " m_first_free_bit: " << m_first_free_bit
                << " m_first_valid_bit: " << m_first_valid_bit
                << " m_max_bit: " << m_max_bit
                << dendl;
}

PmemDev::~PmemDev() {
  // 执行下面这句话可能有重复unmap的问题
  //close_dev();
}

PmemDev *PmemDev::pmem_create_dev(const char *path, const uint64_t size,
                                  CephContext *cct) {
  PmemDev *pmem_dev = nullptr;
  pmem_dev = new PmemDev(path, size, cct);
  if (!pmem_dev) {
    return nullptr;
  }
  return pmem_dev;
}

char *PmemDev::create_dev(const char *path, const uint64_t size,
                          int *is_pmem) {
  char *head_addr = nullptr;

  /* create a pmem file and memory map it */
  if (!(head_addr = (char *)pmem_map_file(path, size, PMEM_FILE_CREATE, 0666,
                    &m_mapped_len, is_pmem))) {
    lderr(m_cct) << "failed to map new pmem file." << dendl;
    return nullptr;
  }
  return head_addr;
}

PmemDev *PmemDev::pmem_open_dev(const char *path, CephContext *cct) {
  PmemDev *pmem_dev = nullptr;
  pmem_dev = new PmemDev(path, cct);
  if (!pmem_dev) {
    return nullptr;
  }
  return pmem_dev;
}

char *PmemDev::open_dev(const char *path, int *is_pmem) {
  char *head_addr = nullptr;

  /* Memory map an existing file */
  if (!(head_addr = (char *)pmem_map_file(path, 0, 0, 0, &m_mapped_len,
                    is_pmem))) {
    lderr(m_cct) << "failed to map existing pmem file." << dendl;
    return nullptr;
  }
  return head_addr;
}

void PmemDev::close_dev() {
  pmem_unmap(m_head_addr, m_mapped_len);
}

void PmemDev::init_data_bit(uint64_t root_space_size){
  m_first_date_bit = size_to_bit(root_space_size) + 1; // arrive to next bit which is free, it map to entry index[0]
  m_first_free_bit = m_first_valid_bit = m_first_date_bit;
  ldout(m_cct, 5) << " init space. "
                  << " m_first_date_bit: " << m_first_date_bit
                  << " m_first_free_bit: " << m_first_free_bit
                  << " m_first_valid_bit: " << m_first_valid_bit
                  << dendl;
}

void PmemDev::set_data_bit(uint64_t root_space_size, uint64_t first_valid_bit,
                  uint64_t last_valid_entry_start_bit,
                  uint64_t last_valid_entry_size) {
  m_first_date_bit = size_to_bit(root_space_size) + 1; // arrive to next bit which is free, it map to entry index[0]
  m_first_valid_bit = first_valid_bit;
  m_first_free_bit = last_valid_entry_start_bit +
                      size_to_bit(last_valid_entry_size) + 1; // to next bit which is free
  ldout(m_cct, 5) << "successfully map pmem Device. "
                  << " m_first_date_bit: " << m_first_date_bit
                  << " m_first_free_bit: " << m_first_free_bit
                  << " m_first_valid_bit: " << m_first_valid_bit
                  << dendl;
}

uint64_t PmemDev::alloc(uint64_t size) {
  // 非头部m_first_free_bit到m_first_valid_bit或到尾部有足够的空间
  if ((m_first_free_bit + size_to_bit(size) <= m_max_bit &&
       m_first_valid_bit <= m_first_free_bit) ||
      (m_first_free_bit + size_to_bit(size) <= m_first_valid_bit &&
       m_first_valid_bit >= m_first_free_bit)) {
    m_first_free_bit += size_to_bit(size);
    //assert(m_max_bit >= m_first_free_bit);
    // 下面这句打印后期可以删掉
    ldout(m_cct, 5) << "alloc in middle, m_first_free_bit: "
                  << m_first_free_bit
                  << dendl;
    return m_first_free_bit - size_to_bit(size);
  } else if (m_first_free_bit + size_to_bit(size) > m_max_bit &&
             m_first_date_bit + size_to_bit(size) <= m_first_valid_bit &&
             m_first_valid_bit <= m_first_free_bit) {
    // 在头部
    // assert(tail >= head)
    m_first_free_bit = m_first_date_bit + size_to_bit(size);
    // 下面这句打印后期可以删掉
    ldout(m_cct, 5) << "alloc in head, m_first_free_bit: "
                  << m_first_free_bit
                  << dendl;
    return m_first_date_bit;
  }

  ldout(m_cct, 5) << size << "alloc fail, no space." << dendl;
  return 0;
}

// release应该使用地址操作，防止少释放曾经跳过的尾部
// pmem可以覆盖写，因此只需要将要回收的区域标记为可使用
// 一组连续的release，只需前进到最后一个释放者的地址
void PmemDev::release_to_here(uint64_t last_retire_entry_bit,
                              uint64_t last_retire_entry_size) {
  // 需要判断是否到了尾部吗？
  // 需要判断释放过头的assert错误？
  // 修改地址为bit位置
  m_first_valid_bit = last_retire_entry_bit +
                      size_to_bit(last_retire_entry_size);
  ldout(m_cct, 5) << "release to m_first_valid_bit: " << m_first_valid_bit
                  << " last_retire_entry_bit: " << last_retire_entry_bit
                  << " last_retire_entry_size: " << last_retire_entry_size
                  << dendl;
}

char *PmemDev::get_head_addr() {
  return m_head_addr;
}

size_t PmemDev::get_mapped_len() {
  return m_mapped_len;
}

bool PmemDev::is_pmem() {
  return m_is_pmem;
}

} // namespace rwl
} // namespace pwl
} // namespace cache
} // namespace librbd