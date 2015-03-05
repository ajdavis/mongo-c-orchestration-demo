cmake_minimum_required(VERSION 2.8.4)
project(mongo-c-orchestration-tests)

include_directories(/usr/local/include/libbson-1.0 /usr/local/include/libmongoc-1.0)

set(SOURCE_FILES mongo-c-orchestration-demo.c)
add_executable(mongo-c-orchestration-demo ${SOURCE_FILES})

# link_directories(/usr/local/lib)
find_library(BSON NAMES bson-1.0 PATHS /usr/local/lib/)
find_library(MONGOC NAMES mongoc-1.0 PATHS /usr/local/lib/)

target_link_libraries(mongo-c-orchestration-demo ${MONGOC} ${BSON})
