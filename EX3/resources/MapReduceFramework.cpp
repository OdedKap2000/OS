//
// Created by odedkaplan on 25/05/2020.
//

#include "MapReduceFramework.h>
#include "MapReduceClient.h"
#include <pthread.h>
#include <atomic>
#include "Barrier.h"
typedef void *JobHandle;
enum stage_t
{
    UNDEFINED_STAGE = 0, MAP_STAGE = 1, SHUFFLE_STAGE = 2, REDUCE_STAGE = 3
};
typedef struct
{
    stage_t stage;
    float percentage;
} JobState;
typedef struct
{
    std::atomic<int> atomicStartedCounter;
    std::atomic<int> atomicFinishedCounter;
    std::atomic<int> atomicReducedCounter;
    ThreadContext *contexts;
    pthread_t *threads;
    int threadCount;
    stage_t stage;
    int inputVecLength;
    const InputVec &inputVec;
    const OutputVec &outputVec;
    const MapReduceClient &client;
    pthread_mutex_t outputVecLocker;
    std::map<K2 *, std::vector<V2 *>> *intermediateMap;
    std::vector<K2 *> intermediateMapKeys;
    Barrier *barrier;
} JobContext;
struct ThreadContext
{
    pthread_mutex_t locker;
    JobContext *generalContext;
    std::List<IntermediatePair> outputVec;
};

void *mapPhase()
{
    int inputVecLength = generalContext->inputVecLength;
    int currAtomic = 0;
    while (currAtomic < inputVecLength)
    {
        currAtomic = generalContext->atomicStartedCounter++;
        InputPair pair = (generalContext->inputVec)[currAtomic];
        generalContext->client.map(std::get<0>(pair), std::get<1>(pair), currContext);
    }
}

void *reducePhase(ThreadContext *currContext, JobContext *generalContext)
{
    int keysLength = (generalContext->intermediateMapKeys).size();
    int currAtomic = 0;
    while (currAtomic < keysLength)
    {
        currAtomic = (generalContext->atomicReducedCounter)++;
        K2 *currKey = generalContext->intermediateMapKeys[currAtomic];
        generalContext->client.reduce(currKey, generalContext->intermediateMap[currKey], currContext);
    }
}

void *shuffleThreadRun(void *context)
{
    ThreadContext *currContext = (ThreadContext *) contextArg;
    JobContext *generalContext = currContext->generalContext;
    while (generalContext->atomicFinishedCounter < generalContext->inputVec.size())
    {
        for (int i = 0; i < generalContext->threadCount; i++)
        {
            ThreadContext *curThreadContext = generalContext->contexts[i];
            pthread_mutex_lock(curThreadContext->locker);
            //take out all the pairs and remove them
            std::list<IntermediatePair *>::iterator it = curThreadContext->outputVec.begin();
            while (it != curThreadContext->outputVec.end())
            {
                IntermediatePair *intermediatePair = *it;
                (*generalContext.intermediateMap)[intermediatePair->first].push_back(intermediatePair->second);
                curThreadContext->outputVec.erase(it++);
            }
            pthread_mutex_unlock(curThreadContext->locker);
        }
    }

    generalContext->barrier->barrier();
    reducePhase(currContext, generalContext);
}

void *generalThreadRun(void *contextArg)
{
    ThreadContext *currContext = (ThreadContext *) contextArg;
    JobContext *generalContext = currContext->generalContext;
    mapPhase(currContext, generalContext);

    generalContext->barrier->barrier();

    reducePhase(currContext, generalContext);
}

void emit2(K2 *key, V2 *value, void *context)
{
    ThreadContext *tc = (ThreadContext *) context;
    pthread_mutex_lock(&tc->locker);
    tc.outputVec.push_back(IntermediatePair(key, value));
    pthread_mutex_unlock(&tc->locker);
}

void emit3(K3 *key, V3 *value, void *context)
{
    ThreadContext *tc = (ThreadContext *) context;
    JobContext generalContext = tc->generalContext;
    pthread_mutex_lock(generalContext->outputVecLocker);
    generalContext.outputVec.push_back(OutputPair(key, value));
    pthread_mutex_unlock(generalContext->outputVecLocker);
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel)
{
    pthread_t threads[multiThreadLevel];
    ThreadContext contexts[MT_LEVEL];
    Barrier myBarrier(multiThreadLevel);
    for (int i = 0; i < multiThreadLevel - 1; ++i)
    {
        contexts[i].locker = PTHREAD_MUTEX_INITIALIZER;
        pthread_create(threads + i, NULL, generalThreadRun, contexts + i);
    }
}

void waitForJob(JobHandle job)
{
    JobContext *jobContext = (JobContext *) job;
    for (int i = 0; i < jobContext.threadCount; i++)
    {
        pthread_join(jobContext->threads[i], NULL);
    }
}

void getJobState(JobHandle job, JobState *state)
{
    JobContext *jobContext = (JobContext *) job;
    state->stage = jobContext->stage;
    state->percentage = (float) jobContext->atomicFinishedCounter / jobContext->inputVec.size();
}

void deleteContextsAndThreads(JobContext *jobContext)
{
    for (int i = 0; i < jobContext->threadCount; i++)
    {
        delete jobContext->threads[i];
        delete jobContext->contexts[i];
    }
}

void closeJobHandle(JobHandle job)
{
    waitForJob(job);
    JobContext *jobContext = (JobContext *) job;

    delete jobContext->atomicStartedCounter;
    delete jobContext->atomicFinishedCounter;
    delete jobContext->atomicReducedCounter;
    ThreadContext *contexts;
    pthread_t *threads;
    delete jobContext->outputVecLocker;
    delete jobContext->intermediateMap;
    delete jobContext->intermediateMapKeys;
    //TODO: delete everything you need
}