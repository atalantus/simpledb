# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------

set(TEST_CC
        test/buffer_manager_test.cc
        test/slotted_page_test.cc
        test/segment_test.cc
        test/btree_test.cc
        )

# ---------------------------------------------------------------------------
# Tester
# ---------------------------------------------------------------------------

add_executable(tester test/tester.cc ${TEST_CC})
target_link_libraries(tester simpledb gtest gmock Threads::Threads)

enable_testing()
add_test(simpledb tester)

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------

add_clang_tidy_target(lint_test "${TEST_CC}")
add_dependencies(lint_test gtest)
list(APPEND lint_targets lint_test)
