// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PARENT_CACHER_OBJECT_DISPATCH_H
#define CEPH_LIBRBD_CACHE_PARENT_CACHER_OBJECT_DISPATCH_H

#include "librbd/io/ObjectDispatchInterface.h"
#include "tools/immutable_object_cache/CacheClient.h"
#include "librbd/cache/TypeTraits.h"
#include "tools/immutable_object_cache/Types.h"

namespace librbd {

class ImageCtx;

namespace cache {

// 声明模板函数，默认为ImageCtx类型，不传参的话默认这个,看起来ImageCtx通用来作为此类模板的默认类型
template <typename ImageCtxT = ImageCtx>
class ParentCacheObjectDispatch : public io::ObjectDispatchInterface {
  // mock（模拟） unit testing support
  typedef cache::TypeTraits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::CacheClient CacheClient;

public:
  // 构造一个对象分发器对象
  static ParentCacheObjectDispatch* create(ImageCtxT* image_ctx) {
    return new ParentCacheObjectDispatch(image_ctx);
  }

  // override显示声明需要在派生类重写此函数，否则编译器报错
  ParentCacheObjectDispatch(ImageCtxT* image_ctx);
  ~ParentCacheObjectDispatch() override;

  // 对象分发类的层级，与rbd分发机制有关
  io::ObjectDispatchLayer get_object_dispatch_layer() const override {
    return io::OBJECT_DISPATCH_LAYER_PARENT_CACHE;
  }

  void init(Context* on_finish = nullptr);
  void shut_down(Context* on_finish) {
    m_image_ctx->op_work_queue->queue(on_finish, 0);
  }
  
  //目前只实现了read，其他操作没有实现，return false，会继续下发到下一层
  bool read(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      librados::snap_t snap_id, int op_flags,
      const ZTracer::Trace &parent_trace, ceph::bufferlist* read_data,
      io::ExtentMap* extent_map, int* object_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;

  //其他操作直接return false，如何rbd的分发器分发到下一层
  bool discard(
      uint64_t object_no, uint64_t object_off, uint64_t object_len, 
      const ::SnapContext &snapc, int discard_flags,
      const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
      uint64_t* journal_tid, io::DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) {
    return false;
  }

  bool write(
      uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
      const ::SnapContext &snapc, int op_flags,
      const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
      uint64_t* journal_tid, io::DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) {
    return false;
  }

  bool write_same(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      io::LightweightBufferExtents&& buffer_extents, ceph::bufferlist&& data, 
      const ::SnapContext &snapc, int op_flags,
      const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
      uint64_t* journal_tid, io::DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) {
    return false;
  }

  bool compare_and_write(
      uint64_t object_no, uint64_t object_off, ceph::bufferlist&& cmp_data, 
      ceph::bufferlist&& write_data, const ::SnapContext &snapc, int op_flags,
      const ZTracer::Trace &parent_trace, uint64_t* mismatch_offset,
      int* object_dispatch_flags, uint64_t* journal_tid,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) {
    return false;
  }

  bool flush(
      io::FlushSource flush_source, const ZTracer::Trace &parent_trace,
      uint64_t* journal_id, io::DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) {
    return false;
  }

  bool invalidate_cache(Context* on_finish) {
    return false;
  }

  bool reset_existence_cache(Context* on_finish) {
    return false;
  }

  void extent_overwritten(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      uint64_t journal_tid, uint64_t new_journal_tid) {
  }

  bool get_state() {
    return m_initialized;
  }

  ImageCtxT* get_image_ctx() {
    return m_image_ctx;
  }

  CacheClient* get_cache_client() {
    return m_cache_client;
  }

private:

  int read_object(std::string file_path, ceph::bufferlist* read_data,
                  uint64_t offset, uint64_t length, Context *on_finish);
  void handle_read_cache(
         ceph::immutable_obj_cache::ObjectCacheRequest* ack,
         uint64_t read_off, uint64_t read_len,
         ceph::bufferlist* read_data,
         io::DispatchResult* dispatch_result,
         Context* on_dispatched);
  int handle_register_client(bool reg);
  int create_cache_session(Context* on_finish, bool is_reconnect);

  ImageCtxT* m_image_ctx;
  CacheClient *m_cache_client;
  bool m_initialized;
  std::atomic<bool> m_connecting;
};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::ParentCacheObjectDispatch<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_PARENT_CACHER_OBJECT_DISPATCH_H
