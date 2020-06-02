
#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include <pthread.h>
#include <atomic>
#include <list>
#include "Barrier.h"
#include <iostream>

#define SUCCESS 0
#define SYSTEM_CALL_FAILURE 1

#define SYS_ERROR "system error: "
#define BAD_ALLOC_ERROR "bad alloc\n"
#define MUTEX_LOCK_FAILED "mutex lock failed\n"
#define MUTEX_UNLOCK_FAILED "mutex unlock failed\n"
#define JOIN_FAILED "join failed with error number "
#define PTHREAD_CREATE_ERROR "pthread create error with erro number "
#define DESTROY_FAILED "destroy mutex failed with error number "


//
// Created by odedkaplan on 25/05/2020.
//

typedef void *JobHandle;

struct ThreadContext
{
    pthread_mutex_t locker;
    JobHandle generalContext;
    std::list<IntermediatePair>* outputVec;
};

typedef struct
{
    std::atomic<int> *atomicStartedCounter;
    std::atomic<int> *atomicFinishedCounter;
    std::atomic<int> *atomicReducedCounter;
    ThreadContext *contexts;
    pthread_t *threads;
    int threadCount;
    stage_t stage;
    const InputVec &inputVec;
    OutputVec &outputVec;
    const MapReduceClient &client;
    pthread_mutex_t outputVecLocker;
    pthread_mutex_t stageLocker;
    IntermediateMap *intermediateMap;
    std::vector<K2 *> *intermediateMapKeys;
    Barrier *barrier;
    bool isWaitCalled;
    bool isWaitFinished;
} JobContext;

void mapPhase(ThreadContext *currContext, JobContext *generalContext)
{
    int inputVecLength = generalContext->inputVec.size();
    int currAtomic = (*(generalContext->atomicStartedCounter))++;
    InputPair pair;
    while (currAtomic < inputVecLength)// - 1)
    {
        pair = (generalContext->inputVec)[currAtomic];
        generalContext->client.map(pair.first, pair.second, currContext);
        (*(generalContext->atomicFinishedCounter))++;
        currAtomic = (*(generalContext->atomicStartedCounter))++;
    }
}

void reducePhase(ThreadContext *currContext, JobContext *generalContext)
{
    int keysLength = (generalContext->intermediateMapKeys)->size();
    int currAtomic = (*(generalContext->atomicReducedCounter))++;
    while (currAtomic < keysLength)
    {
        K2 *currKey = generalContext->intermediateMapKeys->at(currAtomic);
        generalContext->client.reduce(currKey, (*generalContext->intermediateMap)[currKey], currContext);
        currAtomic = (*(generalContext->atomicReducedCounter))++;
    }
}


void *shuffleThreadRun(void *contextArg)
{
    ThreadContext *currContext = (ThreadContext *) contextArg;
    JobContext *generalContext = (JobContext *) currContext->generalContext;
    while (*(generalContext->atomicFinishedCounter) < (int)generalContext->inputVec.size() - 1)
    {
        for (int i = 0; i < generalContext->threadCount - 1; i++)
        {
            ThreadContext *curThreadContext = &generalContext->contexts[i];
            if (pthread_mutex_lock(&(curThreadContext->locker)) != SUCCESS)
            {
                std::cerr << SYS_ERROR << MUTEX_LOCK_FAILED;
                exit(SYSTEM_CALL_FAILURE);
            }

            std::list<IntermediatePair>::iterator it = curThreadContext->outputVec->begin();

            while (it != curThreadContext->outputVec->end())
            {
                IntermediatePair intermediatePair = *it;
                (*(generalContext->intermediateMap))[intermediatePair.first].push_back(intermediatePair.second);
                curThreadContext->outputVec->erase(it++);
            }

            if (pthread_mutex_unlock(&(curThreadContext->locker)) != SUCCESS)
            {
                std::cerr << SYS_ERROR << MUTEX_UNLOCK_FAILED;
                exit(SYSTEM_CALL_FAILURE);
            }
        }

    }

    if (pthread_mutex_lock(&(generalContext->stageLocker)) != SUCCESS)
    {
        std::cerr << SYS_ERROR << MUTEX_LOCK_FAILED;
        exit(SYSTEM_CALL_FAILURE);
    }

    generalContext->stage = SHUFFLE_STAGE;

    if (pthread_mutex_unlock(&(generalContext->stageLocker)) != SUCCESS)
    {
        std::cerr << SYS_ERROR << MUTEX_UNLOCK_FAILED;
        exit(SYSTEM_CALL_FAILURE);
    }

    for (int i = 0; i < generalContext->threadCount - 1; i++)
    {
        ThreadContext *curThreadContext = &generalContext->contexts[i];
        if (pthread_mutex_lock(&(curThreadContext->locker)) != SUCCESS)
        {
            std::cerr << SYS_ERROR << MUTEX_LOCK_FAILED;
            exit(SYSTEM_CALL_FAILURE);
        }
        //take out all the pairs and remove them
        std::list<IntermediatePair>::iterator it = curThreadContext->outputVec->begin();
        while (it != curThreadContext->outputVec->end())
        {
            IntermediatePair intermediatePair = *it;
            (*(generalContext->intermediateMap))[intermediatePair.first].push_back(intermediatePair.second);
            curThreadContext->outputVec->erase(it++);
        }
        if (pthread_mutex_unlock(&(curThreadContext->locker)) != SUCCESS)
        {
            std::cerr << SYS_ERROR << MUTEX_UNLOCK_FAILED;
            exit(SYSTEM_CALL_FAILURE);
        }
    }

    for (auto &it : *generalContext->intermediateMap)
    {
        generalContext->intermediateMapKeys->push_back(it.first);
    }

    generalContext->barrier->barrier();
    if (pthread_mutex_lock(&(generalContext->stageLocker)) != SUCCESS)
    {
        std::cerr << SYS_ERROR << MUTEX_LOCK_FAILED;
        exit(SYSTEM_CALL_FAILURE);
    }

    generalContext->stage = REDUCE_STAGE;

    if (pthread_mutex_unlock(&(generalContext->stageLocker)) != SUCCESS)
    {
        std::cerr << SYS_ERROR << MUTEX_UNLOCK_FAILED;
        exit(SYSTEM_CALL_FAILURE);
    }

    reducePhase(currContext, generalContext);

    for (int i = 0; i < generalContext->threadCount - 1; i++)
    {
        int pthreadErrno = pthread_join(generalContext->threads[i], NULL);
        if (pthreadErrno != SUCCESS)
        {
            std::cerr << SYS_ERROR << JOIN_FAILED << pthreadErrno << "\n";
            exit(SYSTEM_CALL_FAILURE);
        }
    }
    return nullptr;
}

void *generalThreadRun(void *contextArg)
{
    ThreadContext *currContext = (ThreadContext *) contextArg;
    JobContext *generalContext = (JobContext *) currContext->generalContext;
    mapPhase(currContext, generalContext);

    generalContext->barrier->barrier();

    reducePhase(currContext, generalContext);
    return nullptr;
}

void emit2(K2 *key, V2 *value, void *context)
{
    ThreadContext *tc = (ThreadContext *) context;
    if (pthread_mutex_lock(&(tc->locker)) != SUCCESS)
    {
        std::cerr << SYS_ERROR << MUTEX_LOCK_FAILED;
        exit(SYSTEM_CALL_FAILURE);
    }
    tc->outputVec->push_back(IntermediatePair(key, value));
    if (pthread_mutex_unlock(&(tc->locker)) != SUCCESS)
    {
        std::cerr << SYS_ERROR << MUTEX_UNLOCK_FAILED;
        exit(SYSTEM_CALL_FAILURE);
    }
}

void emit3(K3 *key, V3 *value, void *context)
{
    ThreadContext *tc = (ThreadContext *) context;
    JobContext *generalContext = (JobContext *) tc->generalContext;
    if (pthread_mutex_lock(&(generalContext->outputVecLocker)) != SUCCESS)
    {
        std::cerr << SYS_ERROR << MUTEX_LOCK_FAILED;
        exit(SYSTEM_CALL_FAILURE);
    }
    generalContext->outputVec.push_back(OutputPair(key, value));
    if (pthread_mutex_unlock(&generalContext->outputVecLocker) != SUCCESS)
    {
        std::cerr << SYS_ERROR << MUTEX_UNLOCK_FAILED;
        exit(SYSTEM_CALL_FAILURE);
    }
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel)
{

    pthread_t* threads = new pthread_t[multiThreadLevel];
    ThreadContext* contexts = new ThreadContext[multiThreadLevel];
    Barrier* myBarrier = new Barrier(multiThreadLevel);
    try
    {
        JobContext *generalContext = new JobContext{
                .atomicStartedCounter = new std::atomic<int>(0),
                .atomicFinishedCounter = new std::atomic<int>(0),
                .atomicReducedCounter = new std::atomic<int>(0),
                .contexts = contexts,
                .threads = threads,
                .threadCount = multiThreadLevel,
                .stage = UNDEFINED_STAGE,
                .inputVec = inputVec,
                .outputVec = outputVec,
                .client = client,
                .outputVecLocker = PTHREAD_MUTEX_INITIALIZER,
                .stageLocker = PTHREAD_MUTEX_INITIALIZER,
                .intermediateMap = new IntermediateMap,
                .intermediateMapKeys = new std::vector<K2 *>,
                .barrier = myBarrier,
                .isWaitCalled = false,
                .isWaitFinished = false
        };

        if (pthread_mutex_lock(&(generalContext->stageLocker)) != SUCCESS)
        {
            std::cerr << SYS_ERROR << MUTEX_LOCK_FAILED;
            exit(SYSTEM_CALL_FAILURE);
        }

        generalContext->stage = UNDEFINED_STAGE;

        if (pthread_mutex_unlock(&(generalContext->stageLocker)) != SUCCESS)
        {
            std::cerr << SYS_ERROR << MUTEX_UNLOCK_FAILED;
            exit(SYSTEM_CALL_FAILURE);
        }

        for (int i = 0; i < multiThreadLevel - 1; ++i)
        {
            contexts[i] = *new ThreadContext;
            contexts[i].locker = PTHREAD_MUTEX_INITIALIZER;
            contexts[i].generalContext = generalContext;
            contexts[i].outputVec = new std::list<IntermediatePair>;
            int pthreadErrno = pthread_create(threads + i, NULL, generalThreadRun, contexts + i);
            if (pthreadErrno != SUCCESS)
            {
                std::cerr << SYS_ERROR << PTHREAD_CREATE_ERROR << pthreadErrno << "\n";
                exit(SYSTEM_CALL_FAILURE);
            }
        }

        contexts[multiThreadLevel - 1] = *new ThreadContext;
        contexts[multiThreadLevel - 1].locker = PTHREAD_MUTEX_INITIALIZER;
        contexts[multiThreadLevel - 1].generalContext = generalContext;

        if (pthread_mutex_lock(&(generalContext->stageLocker)) != SUCCESS)
        {
            std::cerr << SYS_ERROR << MUTEX_LOCK_FAILED;
            exit(SYSTEM_CALL_FAILURE);
        }

        generalContext->stage = MAP_STAGE;

        if (pthread_mutex_unlock(&(generalContext->stageLocker)) != SUCCESS)
        {
            std::cerr << SYS_ERROR << MUTEX_UNLOCK_FAILED;
            exit(SYSTEM_CALL_FAILURE);
        }

        int pthreadErrno = pthread_create(threads + (multiThreadLevel - 1), NULL, &shuffleThreadRun,
                                          contexts + (multiThreadLevel - 1));


        if (pthreadErrno != SUCCESS)
        {
            std::cerr << SYS_ERROR << PTHREAD_CREATE_ERROR << pthreadErrno << "\n";
            exit(SYSTEM_CALL_FAILURE);
        }
        return generalContext;
    }
    catch (const std::bad_alloc &)
    {

        std::cerr << SYS_ERROR << BAD_ALLOC_ERROR;
        exit(SYSTEM_CALL_FAILURE);
    }
}

void waitForJob(JobHandle job)
{
    JobContext *jobContext = (JobContext *) job;
    jobContext->isWaitCalled = true;
    int pthreadErrno = pthread_join(jobContext->threads[jobContext->threadCount - 1], NULL);
    if (pthreadErrno != SUCCESS)
    {
        std::cerr << SYS_ERROR << JOIN_FAILED << pthreadErrno << "\n";
        exit(SYSTEM_CALL_FAILURE);
    }
    jobContext->isWaitFinished = true;
}

void getJobState(JobHandle job, JobState *state)
{
    JobContext *jobContext = (JobContext *) job;
    if (pthread_mutex_lock(&(jobContext->stageLocker)) != SUCCESS)
    {
        std::cerr << SYS_ERROR << MUTEX_LOCK_FAILED;
        exit(SYSTEM_CALL_FAILURE);
    }

    state->stage = jobContext->stage;

    if (pthread_mutex_unlock(&(jobContext->stageLocker)) != SUCCESS)
    {
        std::cerr << SYS_ERROR << MUTEX_UNLOCK_FAILED;
        exit(SYSTEM_CALL_FAILURE);

    }
    state->percentage = (float) (*(jobContext->atomicFinishedCounter)) * 100 / jobContext->inputVec.size();
}

void deleteContextsAndThreads(JobContext *jobContext)
{
    for (int i = 0; i < jobContext->threadCount; i++)
    {
        int pthreadErrno = pthread_mutex_destroy(&(jobContext->contexts[i].locker));
        if (pthreadErrno != SUCCESS)
        {
            std::cerr << SYS_ERROR << DESTROY_FAILED << pthreadErrno << "\n";
            exit(SYSTEM_CALL_FAILURE);
        }
    }
    delete[] jobContext->threads;
    delete[] jobContext->contexts;
}

void closeJobHandle(JobHandle job)
{
    JobContext *jobContext = (JobContext *) job;
    if (!jobContext->isWaitCalled)
        waitForJob(job);
    while (!jobContext->isWaitFinished){}

    delete jobContext->atomicStartedCounter;
    delete jobContext->atomicFinishedCounter;
    delete jobContext->atomicReducedCounter;
    delete jobContext->barrier;
    deleteContextsAndThreads(jobContext);
    int pthreadErrno = pthread_mutex_destroy(&(jobContext->outputVecLocker));
    if (pthreadErrno != SUCCESS)
    {
        std::cerr << SYS_ERROR << DESTROY_FAILED << pthreadErrno << "\n";
        exit(SYSTEM_CALL_FAILURE);
    }
    pthreadErrno = pthread_mutex_destroy(&(jobContext->stageLocker));
    if (pthreadErrno != SUCCESS)
    {
        std::cerr << SYS_ERROR << DESTROY_FAILED << pthreadErrno << "\n";
        exit(SYSTEM_CALL_FAILURE);
    }
    delete jobContext->intermediateMap;
    delete (jobContext->intermediateMapKeys);
}