#define _GNU_SOURCE
#include <dirent.h>     /* Defines DT_* constants */
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <signal.h>
#include <time.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/syscall.h>

#define handle_error(msg) \
       do { perror(msg); exit(EXIT_FAILURE); } while (0)

struct linux_dirent {
   long           d_ino;
   off_t          d_off;
   unsigned short d_reclen;
   char           d_name[];
};

#define BUF_SIZE 1024*1024*5

pthread_t workerThread;
void RunDeleteThread();
void StopDeleteThread();
void * DeleteThread(void*);

void DeleteFile(const char*);

int GetDeletedFiles();
void incrementDeletedFiles();

int StartsWith(const char*, const char*);
double GetLoad();

static void debug(const char *msg);

char cwdPath[PATH_MAX] = "";

double serverLoad[3];

unsigned int deletedFiles = 0;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

int shouldHaltWork = -1;

int allFilesDeleted = 0;

const char * keywords[] = {
    "expiringOfferschangeOfferActivity", "notifyAgentsForTicketsExpireing", "extendOfferStartIfNotApprovedByVendor",
    "markMostPopularOffersWithLabels", "gatherAffiliateStats", "recalcBoughtFields", "smsPaymentNotification", "doFixes",
    "sendPendingSms", "optimizedSendNewsletter", "findMissingVouchers", "updateVsichkiofertiLog", "59", "yesImCron", "notifyDjagi",
    "autoCampaign", "validateUserEmail", "updateAffiliateVoucherStats", "localAutoCampaignRestCities", "cleanSessionDatabase",
    "generateUserEfficiencyStats", "invalidateVouchers", "fixOfferDates", "notifyPaymentExpires", "calculateTotalAnnexIncome",
    "updateAgentEarnings", "calculateProfits", "makeAffStatTables", "updateAffiliateEarnings", "deleteOldSortOrders", "notifySvetlio",
    "setStatusForExpiredTickets", "cleanExpiredHashCodes", "parseHtmlVsichkiOferti", "notifyVoucherExpires", "changeOfferActivity",
    "markExpiringOffersWithLabels", "resetOffer", "updateDailyOfferStats", "generateOfferEfficiencyStats", "updateBrowserCache",
    "expiringOffers"
};

#define n_array (sizeof (keywords) / sizeof (const char *))

int main(int argc, char const *argv[])
{
    unsigned long startTimer, tickTimer;
    void* ret = NULL;
    int i = 0;

    if (argc > 1) {
        realpath(argv[1], cwdPath);
    } else {
        realpath(".", cwdPath);
    }

    printf("Current working directory: %s\n", cwdPath);
    usleep(500000);

    if (GetLoad() >= 7) {
        printf("Load too high (%f). Quitting...\n", GetLoad());
        exit(EXIT_FAILURE);
    }

    printf("Starting worker thread\n");

    startTimer = time(NULL);

    RunDeleteThread();

    printf("Program is running...\n");

    for( ; ; ) {

        sleep(1);

        tickTimer = time(NULL);

        if ((tickTimer - startTimer) >= 3)
        {
            printf("Checking load. Deleted files so far (%i)\n", GetDeletedFiles());

            GetLoad();

            if (serverLoad[0] >= 5) {
                printf("Load too high (%f). Halting work...\n", serverLoad[0]);

                printf("Attempting to stop the thread...\n");

                StopDeleteThread();

                printf("Sleeping for 10 seconds\n");

                sleep(10);

                GetLoad();

                /*if (serverLoad[0] >= 6) {
                    exit(EXIT_FAILURE);
                }

                RunDeleteThread();*/

                while(GetLoad() >= 4)
                {
                    sleep(3);
                    printf("Load remains high...won't resume.\n");
                }

                printf("Resuming...\n");

                for(i = 0; i < 10; i++) {
                    sleep(1);

                    if (GetLoad() <= 5) {
                        RunDeleteThread();
                    }
                }

                if (shouldHaltWork == 1) {
                    printf("Load too high for longer...Quitting...\n");
                    exit(EXIT_FAILURE);
                }
            }

            startTimer = time(NULL);
        }

        if (allFilesDeleted > 0) {
            break;
        }
    }

    printf("Files deletion completed.\n");
    printf("%i file(s) have been deleted.\n", GetDeletedFiles());

    return 0;
}

void RunDeleteThread()
{
    if (shouldHaltWork == 0)
        return;

    char msg[256];
    pthread_mutex_lock(&lock);

    shouldHaltWork = 0;

    pthread_mutex_unlock(&lock);

    pthread_create(&workerThread, NULL, DeleteThread, "processing...");
    pthread_detach(workerThread);

    sprintf(msg, "Created thread with ID %li", workerThread);
    debug(msg);
}

void StopDeleteThread()
{
    char msg[256];

    pthread_mutex_lock(&lock);

    shouldHaltWork = 1;

    pthread_mutex_unlock(&lock);

    printf("Signaling delete thread to stop\n");

    if (pthread_cancel(workerThread) == 0) {
        sprintf(msg, "Killed thread #%li", workerThread);
        debug(msg);
    }

    printf("Signal sent\n");

    // pthread_kill(workerThread, SIGKILL);
}

double GetLoad()
{
    pthread_mutex_lock(&lock);

    getloadavg(serverLoad, 3);

    pthread_mutex_unlock(&lock);

    return serverLoad[0];
}

int StartsWith(const char *pre, const char *str)
{
    return strncmp(pre, str, strlen(pre));
}

void * DeleteThread(void * args)
{
    int fd, nread;
    char buf[BUF_SIZE];
    struct linux_dirent *d;
    int bpos;
    char d_type;
    char * filename;
    int i;
    int running;

    fd = open(cwdPath, O_RDONLY | O_DIRECTORY);
    if (fd == -1)
        handle_error("could not open directory");

    for ( ; ; ) {
        nread = syscall(SYS_getdents, fd, buf, BUF_SIZE);
        if (nread == -1)
            handle_error("getdents failed");

        if (nread == 0)
           break;

        for (bpos = 0; bpos < nread;) {
            d = (struct linux_dirent *) (buf + bpos);
            d_type = *(buf + bpos + d->d_reclen - 1);

            pthread_mutex_lock(&lock);
            running = shouldHaltWork;
            pthread_mutex_unlock(&lock);

            if (running == 1) {
                // we should halt
                printf("I must halt\n");
                pthread_exit(NULL);

                return NULL;
            }

            if (GetLoad() >= 4) {
                printf("Too much load (%f). Pausing...\n", serverLoad[0]);
                sleep(20);
            }

            // printf("Iterating once. File: %s\n", (char*)d->d_name);
            usleep(3000);

            if( d->d_ino != 0 && d_type == DT_REG ) {
                filename = (char *)d->d_name;

                if (running == 1) {
                    printf("I'm halting work since shouldHaltWork = %i at filename\n", shouldHaltWork);
                    sleep(10);
                }
                
                for(i = 0; i < n_array; i++) {
                    if (StartsWith(keywords[i], filename) == 0) {
                        DeleteFile(filename);
                        break;
                    }
                }
            }

            bpos += d->d_reclen;
        }
    }

    allFilesDeleted = 1;

    pthread_exit(NULL);
}

void DeleteFile(const char * filename)
{
    char filepath[PATH_MAX];

    sprintf(filepath, "%s/%s", cwdPath, filename);

    unlink(filepath);

    // deletedFiles++;

    incrementDeletedFiles();

    // printf("Deleting %s. String length is %zx\n", filepath, strlen(filepath));
}

static void debug(const char *msg) {
	time_t t = time(NULL);
	fprintf(stderr, "%s%s\n\n", ctime(&t), msg);
}

int GetDeletedFiles()
{
    int num = 0;

    pthread_mutex_lock(&lock);

    num = deletedFiles;

    pthread_mutex_unlock(&lock);

    return num;
}

void incrementDeletedFiles()
{
    pthread_mutex_lock(&lock);

    deletedFiles++;

    pthread_mutex_unlock(&lock);
}
