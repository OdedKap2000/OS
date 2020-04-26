#include <stdio.h>
#include <setjmp.h>
#include <signal.h>
#include <unistd.h>
#include <sys/time.h>
#inculde "uthreads.h"
#include <queue> 


#define BLOCKED 0
#define RUNNING 1
#define READY 2

using namespace std; 


typedef struct Thread{
    sigjmp_buf env;
	char stack[STACK_SIZE];
    int mode;
	int id;
	int quantsRanUntilNow;
	int priority;
} thread;


class Wrapper { 
private: 
    thread threadArray[MAX_THREAD_NUM];
    int* quantumArray;
	int quantumArraySize;
	thread* runningThread;
	queue<thread> readyThreadsQueue;
	struct itimerval timer;
	
public: 
	Wrapper() 
    { 
		//TODO: fill
    }
	
	//TODO: create setters

	void schedule(int sig){
		
		// TODO: update the queue, and accrodingly the runnig
		
		timer.it_value.tv_usec = quantumArray[runningThread.id];// first time interval, microseconds part
		// Start a virtual timer. It counts down whenever this process is executing.
		if (setitimer (ITIMER_VIRTUAL, &timer, NULL)) {
			printf("setitimer error.");
		}
		
		printf("Scheduled\n");
	}
}; 
  
