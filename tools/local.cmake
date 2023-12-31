# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

set(TOOLS_SRC tools/database_wrapper.cc)

# ---------------------------------------------------------------------------
# Executables
# ---------------------------------------------------------------------------

add_executable(database_wrapper tools/database_wrapper.cc)
target_link_libraries(database_wrapper simpledb Threads::Threads)

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------

add_clang_tidy_target(lint_tools "${TOOLS_SRC}")
list(APPEND lint_targets lint_tools)
