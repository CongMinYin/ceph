# - Find rpma
#
# RPMA_INCLUDE_DIR - Where to find librpma.h
# RPMA_LIBRARIES - List of libraries when using rpma.
# rpma_FOUND - True if rpma found.

#查找包含文件librpma.h的路径，并存储绝对路径，没有找到则为NOTFOUND
find_path(RPMA_INCLUDE_DIR librpma.h)
# 查找制定的预编译库，并存储路径到变量中
find_library(RPMA_LIBRARIES rpma)

# 找到FindPackageHandleStandardArgs.cmake文件，所以可以使用函数find_package_handle_standard_args
include(FindPackageHandleStandardArgs)
# 找rpma安装包，在后面这两个路径中，如果找到，rpma_FOUND会被自动设置为true，DEFAULT_MSG会输出成功找到会失败找到的信息
find_package_handle_standard_args(rpma
  DEFAULT_MSG RPMA_LIBRARIES RPMA_INCLUDE_DIR)

# 设定这两个路径供主程序作为头文件路径和库依赖路径使用
mark_as_advanced(
  RPMA_INCLUDE_DIR
  RPMA_LIBRARIES)

# 找到rpma并且不是rpma::rpma
if(rpma_FOUND AND NOT TARGET rpma::rpma)
# 后面应该是导入链接库
  # add_library(<name> <SHARED|STATIC|MODULE|OBJECT|UNKNOWN> IMPORTED [GLOBAL])
  # 从外部导入动态或静态链接库
  add_library(rpma::rpma UNKNOWN IMPORTED)
  # 设置输出可执行程序/库的名称前缀/后缀，各种属性
  set_target_properties(rpma::rpma PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${RPMA_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${RPMA_LIBRARIES}")
endif()
