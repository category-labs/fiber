find_package(Boost COMPONENTS thread chrono CONFIG QUIET)
if(NOT TARGET Boost::thread)
  message(STATUS "Boost::thread not found — skipping fiber tests")
  return()
endif()

find_library(_boost_utf_static libboost_unit_test_framework.a)
if(NOT _boost_utf_static)
  message(STATUS "Boost.UnitTestFramework static library not found — skipping fiber tests")
  return()
endif()

add_library(boost_unit_test_framework_static STATIC IMPORTED)
set_target_properties(boost_unit_test_framework_static PROPERTIES
  IMPORTED_LOCATION "${_boost_utf_static}"
)
target_include_directories(boost_unit_test_framework_static INTERFACE
  "${Boost_INCLUDE_DIRS}"
)

file(GLOB _fiber_test_sources "test_*.cpp")

foreach(_src ${_fiber_test_sources})
  get_filename_component(_name ${_src} NAME_WE)
  add_executable(${_name} ${_src})
  target_link_libraries(${_name} PRIVATE
    boost_fiber
    Boost::thread
    Boost::chrono
    boost_unit_test_framework_static
  )
  target_compile_features(${_name} PRIVATE cxx_std_11)
  add_test(NAME fiber_${_name} COMMAND ${_name})
endforeach()
