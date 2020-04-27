#include "uthreads.h"
#include <sys/time.h>
#include <setjmp.h>
#include <signal.h>
#include <iostream>
//#include <unistd.h>
#include <list>
#include <new>

#define MAIN_THREAD 0
#define ARBITRARY_SIG 0
#define NON_ZERO 1
#define SUCCESS 0
#define ZERO 0
#define FAILURE -1
#define SYSTEM_CALL_FAILURE 1
#define BLOCKED 0
#define RUNNING 1
#define READY 2
#define MAIN_THREAD_PRIORITY 0
#define DEFAULT_QUANTS_RAN 0
#define nullptr ((void*)0)
#define SYS_ERROR "system error: "
#define LIB_ERROR "thread library error: "
#define QUANTUM_POSITIVE "quantum values must be positive\n"
#define THREAD_NUMBER_EXCEEDED "max thread number exceeded\n"
#define INVALID_THREAD_ID "invalid thread id\n"
#define BLOCKED_MAIN_THREAD "cannot block main thread\n"
#define SIGNAL_MASKING_ERROR "signal masking error\n"
#define SIGSETJMP_ERROR "sigsetjmp error\n"
#define SETITIMER_ERROR "setitimer error\n"
#define SIGLONGJMP_ERROR "siglongjmp error\n"
#define SIGACTION_ERROR "sigaction error\n"
#define BAD_ALLOC_ERROR "bad alloc\n"
#define SIGEMPTYSET_ERROR "sigemptyset error\n"

//TODO: make sure all system calls are checked and exits with SYSTEM_CALL_FAILURE
//TODO: handle over 1M usecs
//TODO: check if priority is over quantumArraySize. In funcs spawn, change priority.
//TODO: check if tid is over MAX_THREAD_NUM in funcs ___

using namespace std;


typedef struct Thread
{
    sigjmp_buf env;
    char stack[STACK_SIZE];
    int mode;
    int id;
    int quantsRanUntilNow;
    int priority;
} thread;


// wrapper part:

thread *threadArray[MAX_THREAD_NUM];
int *quantumArray;
int quantumArraySize;
int sumQuantumRan;
thread *runningThread;
list<thread *> readyThreadsQueue;
struct itimerval timer;

// TODO mask signal in safeNew
thread *safeNew()
{
    try
    {
        thread *newThread = new Thread;
        return newThread;
    }
    catch (bad_alloc)
    {
        cerr << SYS_ERROR << BAD_ALLOC_ERROR;
        exit(SYSTEM_CALL_FAILURE);
    }
}

bool invaildTid(int tid)
{
    if ((tid >= MAX_THREAD_NUM) || (threadArray[tid] == nullptr))
    {
        cerr << LIB_ERROR << INVALID_THREAD_ID;
        return true;
    }
    return false;
}

sigset_t blockTimer()
{
    sigset_t set;
    if ((sigemptyset(&set) == FAILURE) || (sigaddset(&set, SIGVTALRM) == FAILURE) ||
        (sigprocmask(SIG_BLOCK, &set, NULL);== FAILURE))
    {
        cerr << SYS_ERROR << SIGNAL_MASKING_ERROR;
        exit(SYSTEM_CALL_FAILURE);
    }
    return set;
}

void unblockTimer(set)
{
    if (sigprocmask(SIG_UNBLOCK, &set, NULL) == FAILURE)
    {
        cerr << SYS_ERROR << SIGNAL_MASKING_ERROR;
        exit(SYSTEM_CALL_FAILURE);
    }

}

// TODO: check if terminate crashes
int schedule(int sig)
{
    // The situation to get here is that : the timer is called in the running of a certain thread, or the thread made
    // himself blocked or terminated. It means we need to save the env of the running thread, change it's mode to ready and add it to
    // the ready queue. Then set the running thread as a new one from the queue, and jump to it, and start it's timer,
    // and update that it has 1 more running time quant.

    // mask the sigalarm. It is unmasked inside the if that happens after "longjmp"
    sigset_t set = blockTimer();

    // saves the current system environment into the envelope
    int ret_val = sigsetjmp(runningThread->env, NON_ZERO);
    if (ret_val == FAILURE)
    {
        cerr << SYS_ERROR << SIGSETJMP_ERROR;
        exit(SYSTEM_CALL_FAILURE);
    }
    // if ret_val isn't zeroit means it got here from "longjmp" and we want to set the timer and that's it.
    // the logic is that if we got here from "longjmp" we already arranged all the queue and modes of the threads, and
    // just need to start the timer.

    if (ret_val != ZERO)
    {
        // we got here from "longjmp". so just set the timer
        timer.it_value.tv_usec = quantumArray[runningThread->priority];// first time interval, microseconds part
        // Start a virtual timer. It counts down whenever this process is executing.
        if (setitimer(ITIMER_VIRTUAL, &timer, NULL))
        {
            cerr << SYS_ERROR << SETITIMER_ERROR;
            exit(SYSTEM_CALL_FAILURE);
        }

        // unmask the sigalarm
        unblockTimer(set);
        return SUCCESS;
    }

    // if we're here we already saved the system environment into the envelope

    // If we got here bacause the running thread is blocked don't add it to the ready queue
    if (runningThread->mode != BLOCKED)
    {
        runningThread->mode = READY;
        readyThreadsQueue.push_back(runningThread);
    }

    runningThread = nullptr;

    runningThread = readyThreadsQueue.front();
    readyThreadsQueue.pop_front();

    runningThread->mode = RUNNING;
    sumQuantumRan++;
    runningThread->quantsRanUntilNow++;
    if (siglongjmp(runningThread->env, NON_ZERO) == FAILURE)
    {
        cerr << SYS_ERROR << SIGLONGJMP_ERROR;
        exit(SYSTEM_CALL_FAILURE);
    }
}

// end of wrapper part


bool checkArrayPositive(int *quantum_usecs, int size)
{
    for (int i = 0; i < size; ++i)
    {
        if (quantum_usecs[i] <= 0)
        {
            cerr << LIB_ERROR << QUANTUM_POSITIVE;
            return false;
        }
    }
    return true;
}

/*
 * Description: This function initializes the thread library.
 * You may assume that this function is called before any other thread library
 * function, and that it is called exactly once. The input to the function is
 * an array of the length of a quantum in micro-seconds for each priority. 
 * It is an error to call this function with an array containing non-positive integer.
 * size - is the size of the array.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_init(int *quantum_usecs, int size)
{
    if (!checkArrayPositive(quantum_usecs, size))
        return FAILURE;
    quantumArray = quantum_usecs;
    quantumArraySize = size;

    thread *newThread = safeNew();

    newThread->quantsRanUntilNow = DEFAULT_QUANTS_RAN;
    newThread->priority = MAIN_THREAD_PRIORITY;
    newThread->mode = RUNNING;
    int newID = get_new_id();
    newThread->id = newID;
    threadArray[newID] = newThread;
    sumQuantumRan = DEFAULT_QUANTS_RAN;

    struct sigaction sa = {0};

    // Install timer_handler as the signal handler for SIGVTALRM.
    sa.sa_handler = &schedule;
    if (sigaction(SIGVTALRM, &sa, NULL) == FAILURE)
    {
        cerr << SYS_ERROR << SIGACTION_ERROR;
        exit(SYSTEM_CALL_FAILURE);
    }

    schedule(ARBITRARY_SIG);
    return SUCCESS;
}

int get_new_id()
{
    int i = 0;
    while ((i < MAX_THREAD_NUM) && (threadArray[i] != nullptr))
        i++;
    return i;
}

/*
 * Description: This function creates a new thread, whose entry point is the
 * function f with the signature void f(void). The thread is added to the end
 * of the READY threads list. The uthread_spawn function should fail if it
 * would cause the number of concurrent threads to exceed the limit
 * (MAX_THREAD_NUM). Each thread should be allocated with a stack of size
 * STACK_SIZE bytes.
 * priority - The priority of the new thread.
 * Return value: On success, return the ID of the created thread.
 * On failure, return -1.
*/
int uthread_spawn(void (*f)(void), int priority)
{
    int newID = getNewID();
    if (newID == MAX_THREAD_NUM)
    {
        cerr << LIB_ERROR << THREAD_NUMBER_EXCEEDED;
        return FAILURE;
    }

    thread *newThread = safeNew();
    sp = (address_t) newThread->stack + STACK_SIZE - sizeof(address_t);
    pc = (address_t) f;
    if (sigsetjmp(newThread->env, 1) == FAILURE)
    {
        cerr << SYS_ERROR << SIGSETJMP_ERROR;
        exit(SYSTEM_CALL_FAILURE);
    }
    (newThread->env->__jmpbuf)[JB_SP] = translate_address(sp);
    (newThread->env->__jmpbuf)[JB_PC] = translate_address(pc);
    if (sigemptyset(&newThread->env->__saved_mask) == FAILURE)
    {
        cerr << SYS_ERROR << SIGEMPTYSET_ERROR;
        exit(SYSTEM_CALL_FAILURE);
    }

    newThread->quantsRanUntilNow = DEFAULT_QUANTS_RAN;
    newThread->priority = priority;
    newThread->mode = READY;
    newThread->id = newID;
    threadArray[newID] = newThread;

    readyThreadsQueue.push_back(newThread);

    return SUCCESS;
}


/*
 * Description: This function changes the priority of the thread with ID tid.
 * If this is the current running thread, the effect should take place only the
 * next time the thread gets scheduled.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_change_priority(int tid, int priority)
{
    if (invaildTid(tid))
        return FAILURE;
    threadArray[tid]->priority = priority;
    return SUCCESS;
}


int terminate_program()
{
    for (int i = 0; i < MAX_THREAD_NUM; ++i)
    {
        if (threadArray[i] != nullptr)
        {
            delete threadArray[i];
        }
    }
    exit(SUCCESS);
}

/*
 * Description: This function terminates the thread with ID tid and deletes
 * it from all relevant control structures. All the resources allocated by
 * the library for this thread should be released. If no thread with ID tid
 * exists it is considered an error. Terminating the main thread
 * (tid == 0) will result in the termination of the entire process using
 * exit(0) [after releasing the assigned library memory].
 * Return value: The function returns 0 if the thread was successfully
 * terminated and -1 otherwise. If a thread terminates itself or the main
 * thread is terminated, the function does not return.
*/
int uthread_terminate(int tid)
{
    if (invaildTid(tid))
        return FAILURE;

    if (tid == MAIN_THREAD)
    {
        sigset_t set = blockTimer();
        terminate_program();
    }

    if (runningThread == currThread)
    {
        sigset_t set = blockTimer();
    }
    thread *currThread = threadArray[tid];
    readyThreadsQueue.remove(currThread);
    delete currThread;
    threadArray[tid] = nullptr;
    if (runningThread == currThread)
    {
        schedule(ARBITRARY_SIG);
    }
}


/*
 * Description: This function blocks the thread with ID tid. The thread may
 * be resumed later using uthread_resume. If no thread with ID tid exists it
 * is considered as an error. In addition, it is an error to try blocking the
 * main thread (tid == 0). If a thread blocks itself, a scheduling decision
 * should be made. Blocking a thread in BLOCKED state has no
 * effect and is not considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_block(int tid)
{
    if (invaildTid(tid))
        return FAILURE;

    if (tid == MAIN_THREAD)
    {
        cerr << LIB_ERROR << BLOCKED_MAIN_THREAD;
        return FAILURE;
    }

    thread *currThread = threadArray[tid];

    if (runningThread == currThread)
    {
        sigset_t set = blockTimer();
    }

    currThread->mode = BLOCKED;
    if (runningThread == currThread)
    {
        schedule(ARBITRARY_SIG);
        return SUCCESS;
    }
    readyThreadsQueue.remove(currThread);
    return SUCCESS;
}


/*
 * Description: This function resumes a blocked thread with ID tid and moves
 * it to the READY state. Resuming a thread in a RUNNING or READY state
 * has no effect and is not considered as an error. If no thread with
 * ID tid exists it is considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_resume(int tid)
{
    if (invaildTid(tid))
        return FAILURE;

    thread *currThread = threadArray[tid];
    if (currThread->mode != BLOCKED)
        return SUCCESS;
    currThread->mode = READY;
    readyThreadsQueue.push_back(currThread);
    return SUCCESS;
}


/*
 * Description: This function returns the thread ID of the calling thread.
 * Return value: The ID of the calling thread.
*/
int uthread_get_tid()
{
    return runningThread->id;
}


/*
 * Description: This function returns the total number of quantums since
 * the library was initialized, including the current quantum.
 * Right after the call to uthread_init, the value should be 1.
 * Each time a new quantum starts, regardless of the reason, this number
 * should be increased by 1.
 * Return value: The total number of quantums.
*/
int uthread_get_total_quantums()
{
    return sumQuantumRan;
}


/*
 * Description: This function returns the number of quantums the thread with
 * ID tid was in RUNNING state. On the first time a thread runs, the function
 * should return 1. Every additional quantum that the thread starts should
 * increase this value by 1 (so if the thread with ID tid is in RUNNING state
 * when this function is called, include also the current quantum). If no
 * thread with ID tid exists it is considered an error.
 * Return value: On success, return the number of quantums of the thread with ID tid.
 * 			     On failure, return -1.
*/
int uthread_get_quantums(int tid)
{
    if (invaildTid(tid))
        return FAILURE;
    return threadArray[tid]->quantsRanUntilNow;
}