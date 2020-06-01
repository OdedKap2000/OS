//
// Created by odedkaplan on 25/05/2020.
//

#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include <pthread.h>
#include <atomic>
#include <list>
#include "Barrier.h"

//TODO print errors

typedef void *JobHandle;

struct ThreadContext
{
    pthread_mutex_t locker;
    JobHandle generalContext;
    std::list<IntermediatePair> outputVec;
};

typedef struct
{
    std::atomic<int> * atomicStartedCounter;
    std::atomic<int> * atomicFinishedCounter;
    std::atomic<int> * atomicReducedCounter;
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
    while (currAtomic < inputVecLength)
    {
        currAtomic = *(generalContext->atomicStartedCounter)++;
        InputPair pair = (generalContext->inputVec)[currAtomic];
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
    pthread_mutex_lock(&(generalContext->stageLocker));

    generalContext->stage = UNDEFINED_STAGE;

    pthread_mutex_unlock(&(generalContext->stageLocker));
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
            pthread_mutex_lock(&(curThreadContext->locker));
            //take out all the pairs and remove them
            std::list<IntermediatePair>::iterator it = curThreadContext->outputVec.begin();
            while (it != curThreadContext->outputVec.end())
            {
                IntermediatePair intermediatePair = *it;
                (*(generalContext->intermediateMap))[intermediatePair.first].push_back(intermediatePair.second);
                curThreadContext->outputVec.erase(it++);
            }
            pthread_mutex_unlock(&(curThreadContext->locker));
        }
    }

    pthread_mutex_lock(&(generalContext->stageLocker));

    generalContext->stage = SHUFFLE_STAGE;

    pthread_mutex_unlock(&(generalContext->stageLocker));

    for (int i = 0; i < generalContext->threadCount; i++)
    {
        ThreadContext *curThreadContext = &generalContext->contexts[i];
        pthread_mutex_lock(&(curThreadContext->locker));
        //take out all the pairs and remove them
        std::list<IntermediatePair>::iterator it = curThreadContext->outputVec.begin();
        while (it != curThreadContext->outputVec.end())
        {
            IntermediatePair intermediatePair = *it;
            (*(generalContext->intermediateMap))[intermediatePair.first].push_back(intermediatePair.second);
            curThreadContext->outputVec.erase(it++);
        }
        pthread_mutex_unlock(&(curThreadContext->locker));
    }

//    for(std::map<K2 *, std::vector<V2 *>>::iterator it = generalContext->intermediateMap.begin(); it != generalContext->intermediateMap.end(); ++it) {
//        v.push_back(it->first);
//        cout << it->first << "\n";
//    }
    for (auto &it : *generalContext->intermediateMap)
    {
        generalContext->intermediateMapKeys->push_back(it.first);
    }

    generalContext->barrier->barrier();
    pthread_mutex_lock(&(generalContext->stageLocker));

    generalContext->stage = REDUCE_STAGE;

    pthread_mutex_unlock(&(generalContext->stageLocker));

    reducePhase(currContext, generalContext);

    for (int i = 0; i < generalContext->threadCount - 1; i++)
    {
        pthread_join(generalContext->threads[i], NULL);
    }
}
void *generalThreadRun(void *contextArg)
{
    ThreadContext *currContext = (ThreadContext *) contextArg;
    JobContext *generalContext = (JobContext *)currContext->generalContext;
    mapPhase(currContext, generalContext);

    generalContext->barrier->barrier();

    reducePhase(currContext, generalContext);
}

void emit2(K2 *key, V2 *value, void *context)
{
    ThreadContext *tc = (ThreadContext *) context;
    pthread_mutex_lock(&(tc->locker));
    tc->outputVec.push_back(IntermediatePair(key, value));
    pthread_mutex_unlock(&tc->locker);
}

void emit3(K3 *key, V3 *value, void *context)
{
    ThreadContext *tc = (ThreadContext *) context;
    JobContext *generalContext = (JobContext*)tc->generalContext;
    pthread_mutex_lock(&(generalContext->outputVecLocker));
    generalContext->outputVec.push_back(OutputPair(key, value));
    pthread_mutex_unlock(&generalContext->outputVecLocker);
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel)
{

    pthread_t threads[multiThreadLevel];
    ThreadContext contexts[multiThreadLevel];
    Barrier myBarrier(multiThreadLevel);
    // TODO delete atomics
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
            .intermediateMapKeys = new std::vector<K2*>,
            .barrier = &myBarrier,
            .isWaitCalled = false
    };


    for (int i = 0; i < multiThreadLevel - 1; ++i)
    {
        contexts[i] = *new ThreadContext;
        contexts[i].locker = PTHREAD_MUTEX_INITIALIZER;
        pthread_create(threads + i, NULL, generalThreadRun, contexts + i);
    }

    pthread_create(threads + (multiThreadLevel - 1), NULL, &shuffleThreadRun, contexts + (multiThreadLevel - 1));
    return generalContext;
}

void waitForJob(JobHandle job)
{
    JobContext *jobContext = (JobContext *) job;
    jobContext->isWaitCalled = true;
    pthread_join(jobContext->threads[jobContext->threadCount-1], NULL);
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