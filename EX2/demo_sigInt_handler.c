/*
 * SIGINT signal handler demo program.
 * Hebrew University OS course.
 * Author: OS, os@cs.huji.ac.il
 */

#include <stdio.h>
#include <signal.h>
#include <unistd.h>

//Changes by Gal to mask the signal it handels
void catch_int(int sigNum) {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigprocmask(SIG_BLOCK, &set, NULL);

    // Install catch_int as the signal handler for SIGINT.
    printf(" Don't do that!\n");
    fflush(stdout);

    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigprocmask(SIG_UNBLOCK, &set, NULL);
}


int main(void)
{
    // Install catch_int as the signal handler for SIGINT.
    struct sigaction sa;
    sa.sa_handler = &catch_int;
    if (sigaction(SIGINT, &sa,NULL) < 0) {
        printf("sigaction error.");
    }

    for(;;) {
        pause(); // wait until receiving a signal
    }
    return 0;
}
