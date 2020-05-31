//
// Created by odedkaplan on 25/05/2020.
//

#include "MapReduceFramework.h>
#include "MapReduceClient.h"
#include <pthread.h>
#include <atomic>

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
    std::map<K2*,std::vector<V2*>> *intermediateMap;
    std::vector<K2*> intermediateMapKeys;

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
    while (currAtomic < inputVecLength){
        currAtomic = (generalContext->atomicStartedCounter)++;
        InputPair pair = (generalContext->inputVec)[currAtomic];
        generalContext->client.map(std::get<0>(pair), std::get<1>(pair), currContext);
    }
}

void *reducePhase(ThreadContext *currContext, JobContext *generalContext){
    int keysLength =(generalContext->intermediateMapKeys).size();
    int currAtomic = 0;
    while (currAtomic < keysLength){
        currAtomic = (generalContext->atomicReducedCounter)++;
        K2* currKey = generalContext->intermediateMapKeys[currAtomic];
        generalContext->client.reduce(currKey, generalContext->intermediateMap[currKey], currContext);
    }
}

void *generalThreadRun(void *contextArg)
{
    ThreadContext *currContext = (ThreadContext *) contextArg;
    JobContext *generalContext = currContext->generalContext;

    mapPhase(currContext, generalContext);

    //todo activate barrier



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
    for (int i = 0; i < multiThreadLevel; ++i)
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
    state->stage = job.stage;
    state->percentage = (float) job.atomicFinishedCounter / job.inputVec.size();
}

void closeJobHandle(JobHandle job)
{
    JobContext *jobContext = (JobContext *) job;
    //TODO: delete everything you need
}