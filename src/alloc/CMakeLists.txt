if(WITH_BLUESTORE OR WITH_RBD_RWL)
list(APPEND liballoc_srcs
  Allocator.cc
  AvlAllocator.cc
  BitmapAllocator.cc
  BitmapFreelistManager.cc
  fastbmap_allocator_impl.cc
  FreelistManager.cc
  HybridAllocator.cc
  StupidAllocator.cc
  Types.cc)
endif()

if(HAVE_LIBZBC)
  list(APPEND liballoc_srcs
    zoned_types.cc
    ZonedFreelistManager.cc
    ZonedAllocator.cc)
endif()

add_library(alloc ${liballoc_srcs})
target_include_directories(alloc PRIVATE "./")

if(WITH_EVENTTRACE)
  add_dependencies(alloc eventtrace_tp)
endif()

