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
    ThreadContext *contexts;
    pthread_t *threads;
    int threadCount;
    stage_t stage;
    int inputVecLength;
} JobContext;

struct ThreadContext
{
    pthread_mutex_t locker;
    JobContext *generalContext;
};

void* generalThreadRun(void* contextArg){
    ThreadContext* currContext = (ThreadContext*) contextArg;
    JobContext *generalContext = currContext->generalContext;
    int currAtomic = 0;
    int inputVecLength = generalContext->inputVecLength;
    while (currAtomic < inputVecLength ){
        currAtomic = generalContext->atomicStartedCounter++;


    }

}

void emit2(K2 *key, V2 *value, void *context)
{

}

void emit3(K3 *key, V3 *value, void *context)
{


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
    state->percentage = (float) job.atomicFinishedCounter / job.inputVecLength;
}

void closeJobHandle(JobHandle job)
{
    JobContext *jobContext = (JobContext *) job;
    //TODO: delete everything you need
}