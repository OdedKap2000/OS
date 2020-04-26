#include "uthreads.h"
#include <string>
#include <iostream>
#define FAILURE -1
#define SUCCESS 0


void printError(int result, int expected, const std::string& msg)
{
    if(result == expected)
        return;
    std::cerr << "==================================================" << std::endl;
    std::cerr << "FAILED TEST WITH MSG " << msg << std::endl;
    std::cerr << "==================================================" << std::endl;
    exit(1);
}

void f(void)
{
    while(true) {}
}

//some simple tests
int main()
{
    int result, id;
    int wrongUsecs[] = {1000, -1, 200};
    int wrongUsecs2[] = {1000, 0, 200};
    int usecs[] = {1000, 500, 200};
    //init test
    result = uthread_init(usecs, -1);
    printError(result, FAILURE, "you accepted negative size in function init when it shouldn't be allowed");
    result = uthread_init(usecs, 0);
    printError(result, FAILURE, "you accepted 0 size in function init when it shouldn't be allowed");
    result = uthread_init(wrongUsecs, 3);
    printError(result, FAILURE, "you accepted array with negative values in function init when it shouldn't be allowed");
    result = uthread_init(wrongUsecs2, 3);
    printError(result, FAILURE, "you accepted array with zero values in function init when it shouldn't be allowed");
    result = uthread_init(usecs, 3);
    printError(result, SUCCESS, "you failed when inputs were valid in function init");

    //check usecs
    result = uthread_get_total_quantums();
    printError(result, 1, "you have a wrong starting quantums");
    result = uthread_get_quantums(0);
    printError(result, 1, "you have a wrong starting quantums for thread 0");
    result = uthread_get_quantums(-1);
    printError(result, FAILURE, "you returned quantums for thread with id -1");
    result = uthread_get_quantums(1);
    printError(result, FAILURE, "you returned quantums for thread with id 1");
    result = uthread_get_quantums(MAX_THREAD_NUM);
    printError(result, FAILURE, "you returned quantums for thread with id 1");

    //check running
    result = uthread_get_tid();
    printError(result, 0, "the wrong thread is currently running");

    //check spawn
    result = uthread_spawn(f, -1);
    printError(result, FAILURE, "you accepted negative priority in function spawn when it shouldn't be allowed");
    result = uthread_spawn(f, 3);
    printError(result, FAILURE, "you accepted priority bigger than size of usecs in function spawn when it shouldn't be allowed");
    //check id's in spawn
    for(int i = 1; i < MAX_THREAD_NUM; i++)
    {
        id = uthread_spawn(f, 0);
        printError(id, i, "you failed when inputs were valid in function spawn");
        result = uthread_block(id);
        printError(result, SUCCESS, "you failed to block an existing thread in function block");
    }
    //can't create more id's because all of them are taken
    id = uthread_spawn(f, 0);
    printError(id, FAILURE, "you allowed to create more than MAX_THREAD_NUM id's in function spawn");

    //delete a thread and check you get its id
    result = uthread_terminate(51);
    printError(result, SUCCESS, "you failed to terminate a non running thread");
    id = uthread_spawn(f, 1);
    printError(id, 51, "you failed to create the correct id in function spawn");
    result = uthread_block(id);
    printError(result, SUCCESS, "you failed to block an existing thread in function block");

    //check priority
    result = uthread_change_priority(51, -1);
    printError(result, FAILURE, "you accepted a negative priority in function change priority");
    result = uthread_change_priority(51, 3);
    printError(result, FAILURE, "you accepted a priority bigger than size of usecs in function change priority");
    result = uthread_change_priority(51, 0);
    printError(result, SUCCESS, "you failed to change legal priority in function change priority");
    result = uthread_change_priority(MAX_THREAD_NUM, 0);
    printError(result, FAILURE, "you accepted an invalid id in function change priority");
    result = uthread_change_priority(-1, 0);
    printError(result, FAILURE, "you accepted an invalid id in function change priority");
    result = uthread_terminate(51);
    printError(result, SUCCESS, "you failed to terminate a non running thread");
    result = uthread_change_priority(51, 0);
    printError(result, FAILURE, "you accepted an invalid id in function change priority");

    //testing block
    result = uthread_block(-1);
    printError(result, FAILURE, "you blocked thread -1");
    result = uthread_block(MAX_THREAD_NUM);
    printError(result, FAILURE, "you blocked thread with not existing id");
    result = uthread_block(51);
    printError(result, FAILURE, "you blocked thread with not existing id");
    result = uthread_block(0);
    printError(result, FAILURE, "you blocked thread 0");
    result = uthread_block(1);
    printError(result, SUCCESS, "you failed to block an existing thread");

    //testing terminate
    result = uthread_terminate(-1);
    printError(result, FAILURE, "you accepted to terminate a non existing id");
    result = uthread_terminate(MAX_THREAD_NUM);
    printError(result, FAILURE, "you accepted to terminate a non existing id");
    for(int i = 1; i < MAX_THREAD_NUM; i++)
    {
        if(i != 51)
        {
            result = uthread_terminate(i);
            printError(result, SUCCESS, "you failed to terminate a non running thread");
        }
        else
        {
            result = uthread_terminate(i);
            printError(result, FAILURE, "you accepted to terminate a non existing id");
        }
    }
    std::cout << "successfully passed all tests" << std::endl;
    uthread_terminate(0);
}
