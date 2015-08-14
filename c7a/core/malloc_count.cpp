/*******************************************************************************
 * c7a/core/malloc_count.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2013-2014 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <c7a/core/malloc_count.hpp>

#include <dlfcn.h>
#include <locale.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

namespace c7a {
namespace core {

//! user-defined options for output malloc()/free() operations to stderr

static const int log_operations = 0;    //! <-- set this to 1 for log output
static const size_t log_operations_threshold = 1024 * 1024;

//! option to use gcc's intrinsics to do thread-safe statistics operations
#define THREAD_SAFE_GCC_INTRINSICS      0

//! to each allocation additional data is added for bookkeeping. due to
//! alignment requirements, we can optionally add more than just one integer.
static const size_t alignment = 16; /* bytes (>= 2*sizeof(size_t)) */

//! function pointer to the real procedures, loaded using dlsym
typedef void* (* malloc_type)(size_t);
typedef void (* free_type)(void*);
typedef void* (* realloc_type)(void*, size_t);

static malloc_type real_malloc = NULL;
static free_type real_free = NULL;
static realloc_type real_realloc = NULL;

//! a sentinel value prefixed to each allocation
static const size_t sentinel = 0xDEADC0DE;

//! a simple memory heap for allocations prior to dlsym loading
#define INIT_HEAP_SIZE 1024 * 1024
static char init_heap[INIT_HEAP_SIZE];
static size_t init_heap_use = 0;
static const int log_operations_init_heap = 0;

//! output
#define PPREFIX "malloc_count ### "

/*****************************************/
/* run-time memory allocation statistics */
/*****************************************/

static long long peak = 0, curr = 0, total = 0, num_allocs = 0;

//! add allocation to statistics
static void inc_count(size_t inc) {
#if THREAD_SAFE_GCC_INTRINSICS
    long long mycurr = __sync_add_and_fetch(&curr, inc);
    if (mycurr > peak) peak = mycurr;
    total += inc;
#else
    if ((curr += inc) > peak) peak = curr;
    total += inc;
#endif
    ++num_allocs;
}

//! decrement allocation to statistics
static void dec_count(size_t dec) {
#if THREAD_SAFE_GCC_INTRINSICS
    long long mycurr = __sync_sub_and_fetch(&curr, dec);
#else
    curr -= dec;
#endif
}

//! user function to return the currently allocated amount of memory
size_t malloc_count_current(void) {
    return curr;
}

//! user function to return the peak allocation
size_t malloc_count_peak(void) {
    return peak;
}

//! user function to reset the peak allocation to current
void malloc_count_reset_peak(void) {
    peak = curr;
}

//! user function to return total number of allocations
size_t malloc_count_num_allocs(void) {
    return num_allocs;
}

//! user function which prints current and peak allocation to stderr
void malloc_count_print_status(void) {
    fprintf(stderr, PPREFIX "current %'lld, peak %'lld\n",
            curr, peak);
}

static __attribute__ ((constructor)) void init(void) {
    char* error;

    setlocale(LC_NUMERIC, ""); //! for better readable numbers

    dlerror();

    real_malloc = (malloc_type)dlsym(RTLD_NEXT, "malloc");
    if ((error = dlerror()) != NULL) {
        fprintf(stderr, PPREFIX "error %s\n", error);
        exit(EXIT_FAILURE);
    }

    real_realloc = (realloc_type)dlsym(RTLD_NEXT, "realloc");
    if ((error = dlerror()) != NULL) {
        fprintf(stderr, PPREFIX "error %s\n", error);
        exit(EXIT_FAILURE);
    }

    real_free = (free_type)dlsym(RTLD_NEXT, "free");
    if ((error = dlerror()) != NULL) {
        fprintf(stderr, PPREFIX "error %s\n", error);
        exit(EXIT_FAILURE);
    }
}

static __attribute__ ((destructor)) void finish(void) {
    fprintf(stderr, PPREFIX
            "exiting, total: %'lld, peak: %'lld, current: %'lld\n",
            total, peak, curr);
}

} // namespace core
} // namespace c7a

/****************************************************/
/* exported symbols that overlay the libc functions */
/****************************************************/

using namespace c7a::core;

//! exported malloc symbol that overrides loading from libc
void * malloc(size_t size) throw () {
    void* ret;

    if (size == 0) return NULL;

    if (real_malloc)
    {
        //! call read malloc procedure in libc
        ret = (*real_malloc)(alignment + size);

        inc_count(size);
        if (log_operations && size >= log_operations_threshold) {
            fprintf(stderr, PPREFIX "malloc(%'lld) = %p   (current %'lld)\n",
                    (long long)size, (char*)ret + alignment, curr);
        }

        //! prepend allocation size and check sentinel
        *(size_t*)ret = size;
        *(size_t*)((char*)ret + alignment - sizeof(size_t)) = sentinel;

        return (char*)ret + alignment;
    }
    else
    {
        if (init_heap_use + alignment + size > INIT_HEAP_SIZE) {
            fprintf(stderr, PPREFIX "init heap full !!!\n");
            exit(EXIT_FAILURE);
        }

        ret = init_heap + init_heap_use;
        init_heap_use += alignment + size;

        //! prepend allocation size and check sentinel
        *(size_t*)ret = size;
        *(size_t*)((char*)ret + alignment - sizeof(size_t)) = sentinel;

        if (log_operations_init_heap) {
            fprintf(stderr, PPREFIX "malloc(%'lld) = %p   on init heap\n",
                    (long long)size, (char*)ret + alignment);
        }

        return (char*)ret + alignment;
    }
}

//! exported free symbol that overrides loading from libc
void free(void* ptr) throw () {
    size_t size;

    if (!ptr) return;   //! free(NULL) is no operation

    if ((char*)ptr >= init_heap &&
        (char*)ptr <= init_heap + init_heap_use)
    {
        if (log_operations_init_heap) {
            fprintf(stderr, PPREFIX "free(%p)   on init heap\n", ptr);
        }
        return;
    }

    if (!real_free) {
        fprintf(stderr, PPREFIX
                "free(%p) outside init heap and without real_free !!!\n", ptr);
        return;
    }

    ptr = (char*)ptr - alignment;

    if (*(size_t*)((char*)ptr + alignment - sizeof(size_t)) != sentinel) {
        fprintf(stderr, PPREFIX
                "free(%p) has no sentinel !!! memory corruption?\n", ptr);
    }

    size = *(size_t*)ptr;
    dec_count(size);

    if (log_operations && size >= log_operations_threshold) {
        fprintf(stderr, PPREFIX "free(%p) -> %'lld   (current %'lld)\n",
                ptr, (long long)size, curr);
    }

    (*real_free)(ptr);
}

//! exported calloc() symbol that overrides loading from libc, implemented using
//! our malloc
void * calloc(size_t nmemb, size_t size) throw () {
    void* ret;
    size *= nmemb;
    if (!size) return NULL;
    ret = malloc(size);
    memset(ret, 0, size);
    return ret;
}

//! exported realloc() symbol that overrides loading from libc
void * realloc(void* ptr, size_t size) throw () {
    void* newptr;
    size_t oldsize;

    if ((char*)ptr >= (char*)init_heap &&
        (char*)ptr <= (char*)init_heap + init_heap_use)
    {
        if (log_operations_init_heap) {
            fprintf(stderr, PPREFIX "realloc(%p) = on init heap\n", ptr);
        }

        ptr = (char*)ptr - alignment;

        if (*(size_t*)((char*)ptr + alignment - sizeof(size_t)) != sentinel) {
            fprintf(stderr, PPREFIX
                    "realloc(%p) has no sentinel !!! memory corruption?\n",
                    ptr);
        }

        oldsize = *(size_t*)ptr;

        if (oldsize >= size) {
            //! keep old area, just reduce the size
            *(size_t*)ptr = size;
            return (char*)ptr + alignment;
        }
        else {
            //! allocate new area and copy data
            ptr = (char*)ptr + alignment;
            newptr = malloc(size);
            memcpy(newptr, ptr, oldsize);
            free(ptr);
            return newptr;
        }
    }

    if (size == 0) { //! special case size == 0 -> free()
        free(ptr);
        return NULL;
    }

    if (ptr == NULL) { //! special case ptr == 0 -> malloc()
        return malloc(size);
    }

    ptr = (char*)ptr - alignment;

    if (*(size_t*)((char*)ptr + alignment - sizeof(size_t)) != sentinel) {
        fprintf(stderr, PPREFIX
                "free(%p) has no sentinel !!! memory corruption?\n", ptr);
    }

    oldsize = *(size_t*)ptr;

    dec_count(oldsize);
    inc_count(size);

    newptr = (*real_realloc)(ptr, alignment + size);

    if (log_operations && size >= log_operations_threshold)
    {
        if (newptr == ptr)
            fprintf(stderr, PPREFIX
                    "realloc(%'lld -> %'lld) = %p   (current %'lld)\n",
                    (long long)oldsize, (long long)size, newptr, curr);
        else
            fprintf(stderr, PPREFIX
                    "realloc(%'lld -> %'lld) = %p -> %p   (current %'lld)\n",
                    (long long)oldsize, (long long)size, ptr, newptr, curr);
    }

    *(size_t*)newptr = size;

    return (char*)newptr + alignment;
}

/******************************************************************************/