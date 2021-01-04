// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_ALLOCATOR_TYPES_H
#define CEPH_ALLOCATOR_TYPES_H

#include <functional>
#include <ostream>
#include "include/ceph_assert.h"
#include "include/mempool.h"
#include "include/interval_set.h"
#include "zoned_types.h"

template <typename OFFS_TYPE, typename LEN_TYPE>
struct bluestore_interval_t
{
  static const uint64_t INVALID_OFFSET = ~0ull;

  OFFS_TYPE offset = 0;
  LEN_TYPE length = 0;

  bluestore_interval_t(){}
  bluestore_interval_t(uint64_t o, uint64_t l) : offset(o), length(l) {}

  bool is_valid() const {
    return offset != INVALID_OFFSET;
  }
  uint64_t end() const {
    return offset != INVALID_OFFSET ? offset + length : INVALID_OFFSET;
  }

  bool operator==(const bluestore_interval_t& other) const {
    return offset == other.offset && length == other.length;
  }

};

/// pextent: physical extent
struct bluestore_pextent_t : public bluestore_interval_t<uint64_t, uint32_t>
{
  bluestore_pextent_t() {}
  bluestore_pextent_t(uint64_t o, uint64_t l) : bluestore_interval_t(o, l) {}
  bluestore_pextent_t(const bluestore_interval_t &ext) :
    bluestore_interval_t(ext.offset, ext.length) {}

  DENC(bluestore_pextent_t, v, p) {
    denc_lba(v.offset, p);
    denc_varint_lowz(v.length, p);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluestore_pextent_t*>& ls);
};
WRITE_CLASS_DENC(bluestore_pextent_t)

std::ostream& operator<<(std::ostream& out, const bluestore_pextent_t& o);

typedef mempool::bluestore_cache_other::vector<bluestore_pextent_t> PExtentVector;

#endif

