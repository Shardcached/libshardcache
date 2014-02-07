#ifndef __ATOMIC_H__
#define __ATOMIC_H__

#define __USE_UNIX98
#include <pthread.h>

#ifdef __MACH__
#include <libkern/OSAtomic.h>
#endif

#ifdef __MACH__

#ifndef SPIN_LOCK
#define SPIN_LOCK(__mutex) OSSpinLockLock(__mutex)
#endif

#ifndef SPIN_UNLOCK
#define SPIN_UNLOCK(__mutex) OSSpinLockUnlock(__mutex)
#endif

#else //__MACH__

#ifndef SPIN_LOCK
#define SPIN_LOCK(__mutex) pthread_spin_lock(__mutex)
#endif

#ifndef SPIN_UNLOCK
#define SPIN_UNLOCK(__mutex) pthread_spin_unlock(__mutex)
#endif

#endif //__MACH__

#ifndef MUTEX_LOCK
#define MUTEX_LOCK(__mutex) pthread_mutex_lock(__mutex) 
#endif

#ifndef MUTEX_UNLOCK
#define MUTEX_UNLOCK(__mutex) pthread_mutex_unlock(__mutex) 
#endif

#ifndef ATOMIC_CAS
#define ATOMIC_CAS(__v, __o, __n) __sync_bool_compare_and_swap(&(__v), (__o), (__n))
#endif

#ifndef ATOMIC_INCREMENT
#define ATOMIC_INCREMENT(__v) (void)__sync_add_and_fetch(&(__v), 1)
#endif

#ifndef ATOMIC_DECREMENT
#define ATOMIC_DECREMENT(__v) (void)__sync_sub_and_fetch(&(__v), 1)
#endif

#ifndef ATOMIC_INCREASE
#define ATOMIC_INCREASE(__v, __n) (void)__sync_add_and_fetch(&(__v), (__n))
#endif

#ifndef ATOMIC_DECREASE
#define ATOMIC_DECREASE(__v, __n) (void)__sync_sub_and_fetch(&(__v), (__n))
#endif

#ifndef ATOMIC_READ
#define ATOMIC_READ(__v) __sync_fetch_and_add(&(__v), 0)
#endif

#ifndef ATOMIC_SET
#define ATOMIC_SET(__v, __n) {\
    int __b = 0;\
    do {\
        __b = __sync_bool_compare_and_swap(&(__v), ATOMIC_READ(__v), (__n));\
    } while (!__b);\
}
#endif

#endif //__ATOMIC_H__
