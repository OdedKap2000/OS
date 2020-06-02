
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

//TODO print errors
//
// Created by odedkaplan on 25/05/2020.
//

typedef void *JobHandle;

struct ThreadContext
{
    pthread_mutex_t locker;
    JobHandle generalContext;
    std::list<IntermediatePair> outputVec;
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
} JobContext;

void mapPhase(ThreadContext *currContext, JobContext *generalContext)
{
    int inputVecLength = generalContext->inputVec.size();
    int currAtomic = 0;
    InputPair pair;
    while (currAtomic < inputVecLength)
    {
        currAtomic = (*(generalContext->atomicStartedCounter))++;
        pair = (generalContext->inputVec)[currAtomic];
        generalContext->client.map(pair.first, pair.second, currContext);
        generalContext->atomicFinishedCounter++;
    }
}

void reducePhase(ThreadContext *currContext, JobContext *generalContext)
{
    int keysLength = (generalContext->intermediateMapKeys)->size();
    int currAtomic = 0;
    while (currAtomic < keysLength)
    {
        currAtomic = *(generalContext->atomicReducedCounter)++;
        K2 *currKey = generalContext->intermediateMapKeys->at(currAtomic);
        generalContext->client.reduce(currKey, (*generalContext->intermediateMap)[currKey], currContext);
    }
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
}


void *shuffleThreadRun(void *contextArg)
{
    //TODO: update shuffle stage
    // TODO: go over the logic
    ThreadContext *currContext = (ThreadContext *) contextArg;
    JobContext *generalContext = (JobContext *) currContext->generalContext;
    while (*(generalContext->atomicFinishedCounter) < generalContext->inputVec.size())
    {
        for (int i = 0; i < generalContext->threadCount; i++)
        {
            ThreadContext *curThreadContext = &generalContext->contexts[i];
            if (pthread_mutex_lock(&(curThreadContext->locker)) != SUCCESS)
            {
                std::cerr << SYS_ERROR << MUTEX_LOCK_FAILED;
                exit(SYSTEM_CALL_FAILURE);
            }
            //take out all the pairs and remove them
            std::list<IntermediatePair>::iterator it = curThreadContext->outputVec.begin();
            while (it != curThreadContext->outputVec.end())
            {
                IntermediatePair intermediatePair = *it;
                (*(generalContext->intermediateMap))[intermediatePair.first].push_back(intermediatePair.second);
                curThreadContext->outputVec.erase(it++);
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

    for (int i = 0; i < generalContext->threadCount; i++)
    {
        ThreadContext *curThreadContext = &generalContext->contexts[i];
        if (pthread_mutex_lock(&(curThreadContext->locker)) != SUCCESS)
        {
            std::cerr << SYS_ERROR << MUTEX_LOCK_FAILED;
            exit(SYSTEM_CALL_FAILURE);
        }
        //take out all the pairs and remove them
        std::list<IntermediatePair>::iterator it = curThreadContext->outputVec.begin();
        while (it != curThreadContext->outputVec.end())
        {
            IntermediatePair intermediatePair = *it;
            (*(generalContext->intermediateMap))[intermediatePair.first].push_back(intermediatePair.second);
            curThreadContext->outputVec.erase(it++);
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
}

void *generalThreadRun(void *contextArg)
{
    ThreadContext *currContext = (ThreadContext *) contextArg;
    JobContext *generalContext = (JobContext *) currContext->generalContext;
    mapPhase(currContext, generalContext);

    generalContext->barrier->barrier();

    reducePhase(currContext, generalContext);
}

void emit2(K2 *key, V2 *value, void *context)
{
    ThreadContext *tc = (ThreadContext *) context;
    if (pthread_mutex_lock(&(tc->locker)) != SUCCESS)
    {
        std::cerr << SYS_ERROR << MUTEX_LOCK_FAILED;
        exit(SYSTEM_CALL_FAILURE);
    }
    tc->outputVec.push_back(IntermediatePair(key, value));
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

    pthread_t threads[multiThreadLevel];
    ThreadContext contexts[multiThreadLevel];
    Barrier myBarrier(multiThreadLevel);
    // TODO delete atomics
    try
    {
        JobContext *generalContext = new JobContext{
                .atomicStartedCounter = new std::atomic<int>(0),
                .atomicFinishedCounter = new std::atomic<int>(0),
                .atomicReducedCounter = new std::atomic<int>(0),
                .contexts = contexts,
                .threads = threads,
                .threadCount = multiThreadLevel,
                .stage = MAP_STAGE,
                .inputVec = inputVec,
                .outputVec = outputVec,
                .client = client,
                .outputVecLocker = PTHREAD_MUTEX_INITIALIZER,
                .stageLocker = PTHREAD_MUTEX_INITIALIZER,
                .intermediateMap = new IntermediateMap,
                .intermediateMapKeys = new std::vector<K2 *>,
                .barrier = &myBarrier,
                .isWaitCalled = false
        };


        for (int i = 0; i < multiThreadLevel - 1; ++i)
        {
            contexts[i] = *new ThreadContext;
            contexts[i].locker = PTHREAD_MUTEX_INITIALIZER;
            contexts[i].generalContext = generalContext;
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
}

void getJobState(JobHandle job, JobState *state)
{
    JobContext *jobContext = (JobContext *) job;
    pthread_mutex_lock(&(jobContext->stageLocker));

    state->stage = jobContext->stage;

    pthread_mutex_unlock(&(jobContext->stageLocker));

    state->percentage = (float) *(jobContext->atomicFinishedCounter) / jobContext->inputVec.size();
}

void deleteContextsAndThreads(JobContext *jobContext)
{
    //TODO delete atomics
    for (int i = 0; i < jobContext->threadCount; i++)
    {
        delete &(jobContext->threads[i]);
        delete &(jobContext->contexts[i]);
    }

}

void closeJobHandle(JobHandle job)
{
    JobContext *jobContext = (JobContext *) job;
    if (!jobContext->isWaitCalled)
        waitForJob(job);

    delete jobContext->atomicStartedCounter;
    delete jobContext->atomicFinishedCounter;
    delete jobContext->atomicReducedCounter;
    deleteContextsAndThreads(jobContext);
    pthread_mutex_destroy(&(jobContext->outputVecLocker));
    pthread_mutex_destroy(&(jobContext->stageLocker));
    delete jobContext->intermediateMap;
    delete &(jobContext->intermediateMapKeys);
}