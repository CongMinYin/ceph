// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "CacheClient.h"
#include "common/Cond.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::CacheClient: " << this << " " \
                           << __func__ << ": "

namespace ceph {
namespace immutable_obj_cache {

  CacheClient::CacheClient(const std::string& file, CephContext* ceph_ctx)
    : m_cct(ceph_ctx), m_io_service_work(m_io_service),
      m_dm_socket(m_io_service), m_ep(stream_protocol::endpoint(file)),
      m_io_thread(nullptr), m_session_work(false), m_writing(false),
      m_reading(false), m_sequence_id(0) {
    // 读取配置，客户端有多少线程，后面会启动这么多的线程
    m_worker_thread_num =
      m_cct->_conf.get_val<uint64_t>(
        "immutable_object_cache_client_dedicated_thread_num");

    // 建立asio网路模型
    if (m_worker_thread_num != 0) {
      m_worker = new boost::asio::io_service();
      m_worker_io_service_work = new boost::asio::io_service::work(*m_worker);
      for (uint64_t i = 0; i < m_worker_thread_num; i++) {
        std::thread* thd = new std::thread([this](){m_worker->run();});
        m_worker_threads.push_back(thd);  //回调函数的线程，asio把任务分发给这个线程
      }
    }
    // 创建缓存
    m_bp_header = buffer::create(get_header_size());
  }

  CacheClient::~CacheClient() {
    stop();
  }

  // asio用的线程，处理读任务，会将任务具体分发给m_worker_threads.push_back(thd)
  void CacheClient::run() {
     m_io_thread.reset(new std::thread([this](){m_io_service.run(); }));
  }

  bool CacheClient::is_session_work() {
    return m_session_work.load() == true;
  }

  int CacheClient::stop() {
    m_session_work.store(false);
    m_io_service.stop();

    if (m_io_thread != nullptr) {
      m_io_thread->join();
    }
    if (m_worker_thread_num != 0) {
      m_worker->stop();
      for (auto thd : m_worker_threads) {
        thd->join();
        delete thd;
      }
      delete m_worker;
    }
    return 0;
  }

  // close domain socket
  void CacheClient::close() {
    m_session_work.store(false);
    boost::system::error_code close_ec;
    m_dm_socket.close(close_ec);
    if (close_ec) {
       ldout(m_cct, 20) << "close: " << close_ec.message() << dendl;
    }
  }

  // sync connect
  int CacheClient::connect() {
    int ret = -1;
    C_SaferCond cond;
    Context* on_finish = new LambdaContext([&cond, &ret](int err) {
      ret = err;
      cond.complete(err);
    });

    connect(on_finish);
    cond.wait();

    return ret;
  }

  // async connect
  void CacheClient::connect(Context* on_finish) {
    m_dm_socket.async_connect(m_ep,
      boost::bind(&CacheClient::handle_connect/*回调*/, this,
                  on_finish, boost::asio::placeholders::error));
  }

  // 输出了连接信息，失败或者成功
  void CacheClient::handle_connect(Context* on_finish,
                                   const boost::system::error_code& err) {
    if (err) {
      ldout(m_cct, 20) << "fails to connect to cache server. error : "
                       << err.message() << dendl;
      fault(ASIO_ERROR_CONNECT, err);
      on_finish->complete(-1);
      return;
    }

    ldout(m_cct, 20) << "successfully connected to cache server." << dendl;
    on_finish->complete(0);
  }

  // 查找对象
  void CacheClient::lookup_object(std::string pool_nspace, uint64_t pool_id,
                                  uint64_t snap_id, std::string oid,
                                  CacheGenContextURef&& on_finish) {
    ldout(m_cct, 20) << dendl;
    // 创造请求
    ObjectCacheRequest* req = new ObjectCacheReadData(RBDSC_READ,
                                    ++m_sequence_id, 0, 0,
                                    pool_id, snap_id, oid, pool_nspace);
    req->process_msg = std::move(on_finish); //move 回调函数，避免拷贝
    req->encode(); //编码压缩

    {
      std::lock_guard locker{m_lock};
      m_outcoming_bl.append(req->get_payload_bufferlist()); //放到请求列表
      ceph_assert(m_seq_to_req.find(req->seq) == m_seq_to_req.end());
      m_seq_to_req[req->seq] = req; //请i去的序列号
    }

    // try to send message to server.
    try_send(); //尝试发送，有线程正在发送就返回，没用就启动线程

    // try to receive ack from server.
    try_receive();
  }

  void CacheClient::try_send() {
    ldout(m_cct, 20) << dendl;
    if (!m_writing.load()) {
      m_writing.store(true);
      send_message();
    }
  }

  void CacheClient::send_message() {
    ldout(m_cct, 20) << dendl;
    bufferlist bl;
    {
      std::lock_guard locker{m_lock};
      bl.swap(m_outcoming_bl); //不拷贝，准备把队列的请求都发出去
      ceph_assert(m_outcoming_bl.length() == 0);
    }

    // send bytes as many as possible.
    boost::asio::async_write(m_dm_socket,
        boost::asio::buffer(bl.c_str(), bl.length()),
        boost::asio::transfer_exactly(bl.length()),
        //回调
        [this, bl](const boost::system::error_code& err, size_t cb) {
        if (err || cb != bl.length()) {
           fault(ASIO_ERROR_WRITE, err);  //具体处理错误
           return;
        }

        ceph_assert(cb == bl.length());

        {
	  std::lock_guard locker{m_lock};
           if (m_outcoming_bl.length() == 0) {
             m_writing.store(false);
             return;
           }
        } // 检查队列如果没有请求了，就结束退出

        // still have left bytes, continue to send.
        send_message();
    });
    try_receive();
  }

  void CacheClient::try_receive() {
    ldout(m_cct, 20) << dendl;
    if (!m_reading.load()) {
      m_reading.store(true);
      receive_message();
    }
  }

  void CacheClient::receive_message() {
    ldout(m_cct, 20) << dendl;
    ceph_assert(m_reading.load());
    read_reply_header(); //消息长度不一定一致，先读头部，告诉我消息多大
  }

  void CacheClient::read_reply_header() {
    ldout(m_cct, 20) << dendl;
    /* create new head buffer for every reply */
    bufferptr bp_head(buffer::create(get_header_size()));
    auto raw_ptr = bp_head.c_str();

    boost::asio::async_read(m_dm_socket,
      boost::asio::buffer(raw_ptr, get_header_size()),
      boost::asio::transfer_exactly(get_header_size()),
      boost::bind(&CacheClient::handle_reply_header,
                  this, bp_head,
                  boost::asio::placeholders::error,
                  boost::asio::placeholders::bytes_transferred));
  }

  void CacheClient::handle_reply_header(bufferptr bp_head,
         const boost::system::error_code& ec,
         size_t bytes_transferred) {
    ldout(m_cct, 20) << dendl;
    if (ec || bytes_transferred != get_header_size()) {
      fault(ASIO_ERROR_READ, ec);
      return;
    }

    ceph_assert(bytes_transferred == bp_head.length()); // 读数据多长

    uint32_t data_len = get_data_len(bp_head.c_str());

    bufferptr bp_data(buffer::create(data_len));
    read_reply_data(std::move(bp_head), std::move(bp_data), data_len);
  }

  void CacheClient::read_reply_data(bufferptr&& bp_head,
                                    bufferptr&& bp_data,
                                    const uint64_t data_len) {
    ldout(m_cct, 20) << dendl;
    auto raw_ptr = bp_data.c_str();
    boost::asio::async_read(m_dm_socket, boost::asio::buffer(raw_ptr, data_len),
      boost::asio::transfer_exactly(data_len),
      boost::bind(&CacheClient::handle_reply_data,
                  this, std::move(bp_head), std::move(bp_data), data_len,
                  boost::asio::placeholders::error,
                  boost::asio::placeholders::bytes_transferred));
  }

  void CacheClient::handle_reply_data(bufferptr bp_head,
                                      bufferptr bp_data,
                                      const uint64_t data_len,
                                      const boost::system::error_code& ec,
                                      size_t bytes_transferred) {
    ldout(m_cct, 20) << dendl;
    if (ec || bytes_transferred != data_len) {
      fault(ASIO_ERROR_WRITE, ec);
      return;
    }
    ceph_assert(bp_data.length() == data_len);

    bufferlist data_buffer;
    data_buffer.append(std::move(bp_head));
    data_buffer.append(std::move(bp_data));

    ObjectCacheRequest* reply = decode_object_cache_request(data_buffer);
    data_buffer.clear();
    ceph_assert(data_buffer.length() == 0);

    process(reply, reply->seq);

    {
      std::lock_guard locker{m_lock};
      if (m_seq_to_req.size() == 0 && m_outcoming_bl.length()) {
        m_reading.store(false);
        return;
      }
    }
    // 如果还有数据继续读
    if (is_session_work()) {
      receive_message();
    }
  }

  void CacheClient::process(ObjectCacheRequest* reply, uint64_t seq_id) {
    ldout(m_cct, 20) << dendl;
    ObjectCacheRequest* current_request = nullptr;
    {
      std::lock_guard locker{m_lock};
      ceph_assert(m_seq_to_req.find(seq_id) != m_seq_to_req.end());
      current_request = m_seq_to_req[seq_id];
      m_seq_to_req.erase(seq_id);
    }

    ceph_assert(current_request != nullptr);
    auto process_reply = new LambdaContext([current_request, reply]
      (bool dedicated) {
       if (dedicated) {
         // dedicated thrad to execute this context.
       }
       current_request->process_msg.release()->complete(reply);
       delete current_request;
       delete reply;
    });

    //下面的complete会执行前面的process_reply，m_worker_thread_num配置的线程池数量
    if (m_worker_thread_num != 0) {
      m_worker->post([process_reply]() {
        process_reply->complete(true);
      }); //丢给线程池
    } else {
      process_reply->complete(false); // 当前线程执行
    }
  }

  //处理错误
  // if there is one request fails, just execute fault, then shutdown RO.
  void CacheClient::fault(const int err_type,
                          const boost::system::error_code& ec) {
    ldout(m_cct, 20) << "fault." << ec.message() << dendl;

    if (err_type == ASIO_ERROR_CONNECT) {
       ceph_assert(!m_session_work.load());
       if (ec == boost::asio::error::connection_refused) {
         ldout(m_cct, 20) << "Connecting RO daenmon fails : "<< ec.message()
                        << ". Immutable-object-cache daemon is down ? "
                        << "Data will be read from ceph cluster " << dendl;
       } else {
         ldout(m_cct, 20) << "Connecting RO daemon fails : "
                        << ec.message() << dendl;
       }

       if (m_dm_socket.is_open()) {
         // Set to indicate what error occurred, if any.
         // Note that, even if the function indicates an error,
         // the underlying descriptor is closed.
         boost::system::error_code close_ec;
         m_dm_socket.close(close_ec);
         if (close_ec) {
           ldout(m_cct, 20) << "close: " << close_ec.message() << dendl;
         }
      }
      return;
    }

    if (!m_session_work.load()) {
      return;
    }

    /* when current session don't work, ASIO will don't receive any new request from hook.
     * On the other hand, for pending request of ASIO, cancle these request,
     * then call their callback. these request which are cancled by this method,
     * will be re-dispatched to RADOS layer.
     * make sure just have one thread to modify execute below code. */
    m_session_work.store(false);

    if (err_type == ASIO_ERROR_MSG_INCOMPLETE) {
       ldout(m_cct, 20) << "ASIO In-complete message." << ec.message() << dendl;
       ceph_assert(0);
    }

    if (err_type == ASIO_ERROR_READ) {
       ldout(m_cct, 20) << "ASIO async read fails : " << ec.message() << dendl;
    }

    if (err_type == ASIO_ERROR_WRITE) {
       ldout(m_cct, 20) << "ASIO asyn write fails : " << ec.message() << dendl;
       // CacheClient should not occur this error.
       ceph_assert(0);
    }

    // currently, for any asio error, just shutdown RO.
    close();

    /* all pending request, which have entered into ASIO,
     * will be re-dispatched to RADOS.*/
    {
      std::lock_guard locker{m_lock};
      for (auto it : m_seq_to_req) {
        it.second->type = RBDSC_READ_RADOS;
        it.second->process_msg->complete(it.second);  //读没成功，执行回调函数，请求会到librbd下一层
      }
      m_seq_to_req.clear();
    }

    ldout(m_cct, 20) << "Because ASIO domain socket fails, just shutdown RO.\
                       Later all reading will be re-dispatched RADOS layer"
                       << ec.message() << dendl;
  }

  //实现的不好， client的端的rbd与read only cache的object的注册
  // TODO : re-implement this method
  int CacheClient::register_client(Context* on_finish) {
    ObjectCacheRequest* reg_req = new ObjectCacheRegData(RBDSC_REGISTER,
                                                         m_sequence_id++);
    reg_req->encode();

    bufferlist bl;
    bl.append(reg_req->get_payload_bufferlist());

    uint64_t ret;
    boost::system::error_code ec;

    ret = boost::asio::write(m_dm_socket,
      boost::asio::buffer(bl.c_str(), bl.length()), ec);

    if (ec || ret != bl.length()) {
      fault(ASIO_ERROR_WRITE, ec);
      return -1;
    }
    delete reg_req;

    ret = boost::asio::read(m_dm_socket,
      boost::asio::buffer(m_bp_header.c_str(), get_header_size()), ec);
    if (ec || ret != get_header_size()) {
      fault(ASIO_ERROR_READ, ec);
      return -1;
    }

    uint64_t data_len = get_data_len(m_bp_header.c_str());
    bufferptr bp_data(buffer::create(data_len));

    ret = boost::asio::read(m_dm_socket, boost::asio::buffer(bp_data.c_str(),
                            data_len), ec);
    if (ec || ret != data_len) {
      fault(ASIO_ERROR_READ, ec);
      return -1;
    }

    bufferlist data_buffer;
    data_buffer.append(m_bp_header);
    data_buffer.append(std::move(bp_data));
    ObjectCacheRequest* req = decode_object_cache_request(data_buffer);
    if (req->type == RBDSC_REGISTER_REPLY) {
      m_session_work.store(true);
      on_finish->complete(0);
    } else {
      on_finish->complete(-1);
    }

    delete req;
    return 0;
  }

}  // namespace immutable_obj_cache
}  // namespace ceph
