set(BENCH_CC
        bench/bm_buffer_manager.cc
        bench/bm_slotted_page.cc
        bench/bm_btree.cc
        )

add_executable(benchmarks bench/benchmark.cc ${BENCH_CC})
target_link_libraries(benchmarks simpledb benchmark gtest gmock Threads::Threads)
