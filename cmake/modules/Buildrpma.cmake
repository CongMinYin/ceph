# 函数，里面是独立的作用域，第一个参数是函数名
function(build_rpma)
  #包含默个cmake库，目测是为了后面可以调用ExternalProject_Add
  include(ExternalProject)
  #设置值，路径
  set(RPMA_SRC "${CMAKE_BINARY_DIR}/src/rpma/src")
  set(RPMA_INCLUDE "${RPMA_SRC}/include")

  # Use debug RPMA libs in debug lib/rbd builds
  if(CMAKE_BUILD_TYPE STREQUAL Debug)
    set(RPMA_LIB_DIR "debug")
  else()
    set(RPMA_LIB_DIR "nondebug")
  endif()
  set(RPMA_LIB "${RPMA_SRC}/${RPMA_LIB_DIR}")

  include(FindMake)
  find_make("MAKE_EXECUTABLE" "make_cmd")

  ExternalProject_Add(rpma_ext
      GIT_REPOSITORY "https://github.com/pmem/rpma.git"
      GIT_TAG "0.9.0"
      GIT_SHALLOW TRUE
      SOURCE_DIR ${CMAKE_BINARY_DIR}/src/rpma
      CMAKE_ARGS ""
      CONFIGURE_COMMAND ""
      BUILD_COMMAND rm -rf build && mkdir build && cd build && cmake .. && make
      #BUILD_COMMAND ${make_cmd}
      BUILD_IN_SOURCE 1
      #.a 表示静态链接库，so表示动态
      BUILD_BYPRODUCTS "${RPMA_LIB}/librpma.a"
      INSTALL_COMMAND cd build && make install)
      #INSTALL_COMMAND "true")

  # librpma
  # 从外部导入静态链接库
  add_library(rpma::rpma STATIC IMPORTED)
  # rpma_ext表示依赖的项目，保证rpma_ext再其他目标之前被构建，这里要在rpma::rpma前被构建
  add_dependencies(rpma::rpma rpma_ext)
  file(MAKE_DIRECTORY ${RPMA_INCLUDE})
  # 设置目标属性，.a表示静态链接库
  set_target_properties(rpma::rpma PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${RPMA_INCLUDE}
    IMPORTED_LOCATION "${RPMA_LIB}/librpma.a"
    INTERFACE_LINK_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})

endfunction()
