# ---------------------------------------------------------------------------
# Includes
# ---------------------------------------------------------------------------

set(
        SRC_CC_LINTER_IGNORE
)
# include("${CMAKE_SOURCE_DIR}/src/this-could-be-your-folder/local.cmake")

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------

set(
        SRC_CC
        src/buffer_manager.cc
        src/database.cc
        src/fsi_segment.cc
        src/hex_dump.cc
        src/posix_file.cc
        src/schema_segment.cc
        src/schema.cc
        src/slotted_page.cc
        src/sp_segment.cc
)

# Gather lintable files
set(SRC_CC_LINTING "")
foreach (SRC_FILE ${SRC_CC})
    list(FIND SRC_CC_LINTER_IGNORE "${SRC_FILE}" SRC_FILE_IDX)
    if (${SRC_FILE_IDX} EQUAL -1)
        list(APPEND SRC_CC_LINTING "${SRC_FILE}")
    endif ()
endforeach ()

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------

add_library(simpledb STATIC ${SRC_CC} ${INCLUDE_H})
target_link_libraries(
        simpledb
        gflags
        Threads::Threads
        ${LLVM_LIBS}
        ${LLVM_LDFLAGS}
)

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------

add_clang_tidy_target(lint_src "${SRC_CC_LINTING}")
list(APPEND lint_targets lint_src)
