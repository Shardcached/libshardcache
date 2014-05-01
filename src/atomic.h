#ifndef __ATOMIC_H__
#define __ATOMIC_H__

#define __USE_UNIX98
#include <pthread.h>

#ifdef __MACH__
#include <libkern/OSAtomic.h>
#endif

#ifndef __MACH__
#define SPIN_INIT(__mutex) pthread_spin_init(__mutex, 0)
#else
#define SPIN_INIT(__mutex)
#endif

#ifndef __MACH__
#define SPIN_DESTROY(__mutex) pthread_spin_destroy(__mutex)
#else
#define SPIN_DESTROY(__mutex)
#endif

#ifndef __MACH__
#define SPIN_LOCK(__mutex) pthread_spin_lock(__mutex)
#else
#define SPIN_LOCK(__mutex) OSSpinLockLock(__mutex)
#endif

#ifndef __MACH__
#define SPIN_UNLOCK(__mutex) pthread_spin_unlock(__mutex)
#else
#define SPIN_UNLOCK(__mutex) OSSpinLockUnlock(__mutex)
#endif

#define MUTEX_INIT(__mutex) pthread_mutex_init(__mutex, NULL)

#define MUTEX_INIT_RECURSIVE(__mutex) {\
    pthread_mutexattr_t __attr; \
    pthread_mutexattr_init(&__attr); \
    pthread_mutexattr_settype(&__attr, PTHREAD_MUTEX_RECURSIVE); \
    pthread_mutex_init(__mutex, &__attr); \
    pthread_mutexattr_destroy(&__attr); \
}

#define MUTEX_DESTROY(__mutex) pthread_mutex_destroy(__mutex)

#define MUTEX_LOCK(__mutex) pthread_mutex_lock(__mutex) 

#define MUTEX_UNLOCK(__mutex) pthread_mutex_unlock(__mutex) 

#define CONDITION_INIT(__cond) pthread_cond_init(__cond, NULL)

#define CONDITION_DESTROY(__cond) pthread_cond_destroy(__cond)

#define CONDITION_WAIT(__c, __m) {\
    MUTEX_LOCK(__m); \
    pthread_cond_wait(__c, __m); \
    MUTEX_UNLOCK(__m); \
}


#define CONDITION_TIMEDWAIT(__c, __m, __t) {\
    MUTEX_LOCK(__m); \
    pthread_cond_timedwait(__c, __m, __t); \
    MUTEX_UNLOCK(__m); \
}

#define CONDITION_WAIT_IF(__c, __m, __e) {\
    MUTEX_LOCK(__m); \
    if (__e) \
        pthread_cond_wait(__c, __m); \
    MUTEX_UNLOCK(__m); \
}

#define CONDITION_WAIT_WHILE(__c, __m, __e) {\
    MUTEX_LOCK(__m); \
    while (__e) \
        pthread_cond_wait(__c, __m); \
    MUTEX_UNLOCK(__m); \
}

#define CONDITION_SIGNAL(__c, __m) {\
    pthread_mutex_lock(__m); \
    pthread_cond_signal(__c); \
    pthread_mutex_unlock(__m); \
}

#define ATOMIC_CAS(__v, __o, __n) __sync_bool_compare_and_swap(&(__v), (__o), (__n))

#define ATOMIC_INCREMENT(__v) (void)__sync_add_and_fetch(&(__v), 1)

#define ATOMIC_DECREMENT(__v) (void)__sync_sub_and_fetch(&(__v), 1)

#define ATOMIC_INCREASE(__v, __n) (void)__sync_add_and_fetch(&(__v), (__n))

#define ATOMIC_DECREASE(__v, __n) (void)__sync_sub_and_fetch(&(__v), (__n))

#define ATOMIC_READ(__v) __sync_fetch_and_add(&(__v), 0)

#define ATOMIC_SET(__v, __n) {\
    int __o = ATOMIC_READ(__v);\
    if (__builtin_expect(__o != (__n), 1)) {\
        int __b = 0;\
        do {\
            __b = ATOMIC_CAS(__v, ATOMIC_READ(__v), __n);\
        } while (__builtin_expect(!__b, 0));\
    }\
}

#define ATOMIC_SET_IF(__v, __c, __n, __t) {\
    __t __o = ATOMIC_READ(__v); \
    while (__builtin_expect((__o __c (__n)) && !ATOMIC_CAS(__v, __o, __n), 0))\
        __o = ATOMIC_READ(__v);\
}

#endif //__ATOMIC_H__
