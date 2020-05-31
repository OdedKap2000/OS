//
// Created by odedkaplan on 25/05/2020.
//

#include "MapReduceFramework.h>
#include "MapReduceClient.h"
#include <pthread.h>
#include <atomic>
#define MASK_TWO_LOWER_BITS 3

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
    std::atomic<int> atomic_counter(0);
    ThreadContext *threadContextList;
} JobContext;

typedef struct
{
    JobContext *currJob;
} ThreadContext;


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
    for (int i = 0; i < multiThreadLevel; ++i)
    {

    }
}

void waitForJob(JobHandle job)
{


}

void getJobState(JobHandle job, JobState *state)
{


}

void closeJobHandle(JobHandle job)
{


}


