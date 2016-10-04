/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/eventfd.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/epoll.h>
#include <stdarg.h>
#include "org_apache_storm_container_cgroup_monitor_CgroupOOMMonitor.h"

#define BUFSIZE 512
#define MAX_EVENTS 64

extern int errno;

// Global references to java classes
static jclass queue_class;
static jclass map_class;
static jclass CgroupMonitoringInfo_class;
static jclass LoggerFactory_class;
static jclass Logger_class;

static jmethodID queue_poll;
static jmethodID queue_size;
static jmethodID queue_isEmpty;
static jmethodID queue_add;

// get map method ids
static jmethodID map_put;
static jmethodID map_remove;

// get CgroupMonitoringInfo_class member variable and method ids
static jfieldID CgroupMonitoringInfo_cgroupId;
static jmethodID CgroupMonitoringInfo_getFullPathToOOMControlFile;
static jmethodID CgroupMonitoringInfo_getFullPathToCgroupEventControlFile;
static jmethodID CgroupMonitoringInfo_setEfd;
static jmethodID CgroupMonitoringInfo_setOfd;
static jmethodID CgroupMonitoringInfo_setEventDataPtr;
static jmethodID CgroupMonitoringInfo_getEfd;
static jmethodID CgroupMonitoringInfo_getOfd;
static jmethodID CgroupMonitoringInfo_getEventDataPtr;

// declare logger
static jobject LOGGER;
// logger method ids
static jmethodID Logger_info;
static jmethodID Logger_debug;
static jmethodID Logger_error;

// global epoll fd
static int epfd;

// declare logging levels
#define ERROR 0
#define INFO 1
#define DEBUG 2

// prototypes
char *get_c_string(JNIEnv *env, jstring jstr);
int register_cgroup(JNIEnv *env, char *event_control_path, char *memory_oom_control, int *ofd, int *efd);
int log_message(JNIEnv *env, int type, char *format, ...);

/*
 * intializing and cache java class, method, and field ids
 */
JNIEXPORT jint JNICALL Java_org_apache_storm_container_cgroup_monitor_CgroupOOMMonitor_initializeJavaApiCallIds
(JNIEnv *env, jobject thisObj) {

    printf("initializling java api call ids for native\n");

    // get java classes
    queue_class = (*env)->NewGlobalRef(env, (*env)->FindClass(env, "java/util/concurrent/ConcurrentLinkedQueue"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    map_class = (*env)->NewGlobalRef(env, (*env)->FindClass(env, "java/util/concurrent/ConcurrentMap"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    CgroupMonitoringInfo_class = (*env)->NewGlobalRef(env, (*env)->FindClass(env, "org/apache/storm/container/cgroup/monitor/CgroupMonitoringInfo"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }

    // get queue method ids
    queue_poll = (*env)->NewGlobalRef(env, (*env)->GetMethodID(env, queue_class, "poll", "()Ljava/lang/Object;"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    queue_size = (*env)->NewGlobalRef(env, (*env)->GetMethodID(env, queue_class, "size", "()I"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    queue_isEmpty = (*env)->NewGlobalRef(env, (*env)->GetMethodID(env, queue_class, "isEmpty", "()Z"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    queue_add = (*env)->NewGlobalRef(env, (*env)->GetMethodID(env, queue_class, "add", "(Ljava/lang/Object;)Z"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    //get map method ids
    map_put = (*env)->NewGlobalRef(env, (*env)->GetMethodID(env, map_class, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    map_remove = (*env)->NewGlobalRef(env, (*env)->GetMethodID(env, map_class, "remove", "(Ljava/lang/Object;)Ljava/lang/Object;"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }

    //get CgroupMonitoringInfo_class member variable and method ids
    CgroupMonitoringInfo_cgroupId =  (*env)->GetFieldID(env, CgroupMonitoringInfo_class, "cgroupId", "Ljava/lang/String;");
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    CgroupMonitoringInfo_getFullPathToOOMControlFile= (*env)->NewGlobalRef(env, (*env)->GetMethodID(env,
        CgroupMonitoringInfo_class, "getFullPathToOOMControlFile", "()Ljava/lang/String;"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    CgroupMonitoringInfo_getFullPathToCgroupEventControlFile =
    (*env)->NewGlobalRef(env, (*env)->GetMethodID(env, CgroupMonitoringInfo_class, "getFullPathToCgroupEventControlFile", "()Ljava/lang/String;"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    CgroupMonitoringInfo_setEfd = (*env)->NewGlobalRef(env, (*env)->GetMethodID(env, CgroupMonitoringInfo_class, "setEfd", "(I)V"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    CgroupMonitoringInfo_setOfd = (*env)->NewGlobalRef(env, (*env)->GetMethodID(env, CgroupMonitoringInfo_class, "setOfd", "(I)V"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    CgroupMonitoringInfo_setEventDataPtr = (*env)->NewGlobalRef(env, (*env)->GetMethodID(env, CgroupMonitoringInfo_class, "setEventDataPtr", "(J)V"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    CgroupMonitoringInfo_getEfd = (*env)->NewGlobalRef(env, (*env)->GetMethodID(env, CgroupMonitoringInfo_class, "getEfd", "()I"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    CgroupMonitoringInfo_getOfd = (*env)->NewGlobalRef(env, (*env)->GetMethodID(env, CgroupMonitoringInfo_class, "getOfd", "()I"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    CgroupMonitoringInfo_getEventDataPtr = (*env)->NewGlobalRef(env, (*env)->GetMethodID(env, CgroupMonitoringInfo_class, "getEventDataPtr", "()J"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }

    // initialize logger classes
    LoggerFactory_class = (*env)->NewGlobalRef(env, (*env)->FindClass(env, "org/slf4j/LoggerFactory"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    Logger_class = (*env)->NewGlobalRef(env, (*env)->FindClass(env, "org/slf4j/Logger"));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    // initialize logger methods
    jmethodID LoggerFactory_getLogger = (*env)->GetStaticMethodID(env, LoggerFactory_class, "getLogger", "(Ljava/lang/Class;)Lorg/slf4j/Logger;");
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    jmethodID Class_getClass = (*env)->GetMethodID(env, CgroupMonitoringInfo_class, "getClass", "()Ljava/lang/Class;");
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    Logger_info = (*env)->GetMethodID(env, Logger_class, "info", "(Ljava/lang/String;)V");
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    Logger_debug = (*env)->GetMethodID(env, Logger_class, "debug", "(Ljava/lang/String;)V");
    if ((*env)->ExceptionCheck(env)) {
        return -1;
    }
    Logger_error = (*env)->GetMethodID(env, Logger_class, "error", "(Ljava/lang/String;)V");
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    // initialize logger object
    jobject clsObj = (*env)->CallObjectMethod(env, thisObj, Class_getClass);
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }
    LOGGER = (*env)->NewWeakGlobalRef(env, (*env)->CallStaticObjectMethod(env, LoggerFactory_class, LoggerFactory_getLogger, clsObj));
    if ((*env)->ExceptionCheck(env)) {
       return -1;
    }

    //create instance of epoll
    epfd = epoll_create (20);
    if (epfd < 0) {
        log_message(env, ERROR, "epoll_create error!");
        return -1;
    }
    return 0;
}

/*
 * Thread for detecting OOM events
 */
JNIEXPORT jint JNICALL Java_org_apache_storm_container_cgroup_monitor_CgroupOOMMonitor_startOOMMonitoringNative
(JNIEnv *env,
    jobject thisObj,
    jobject cgroups_being_monitored,
    jobject notifications) {

    struct epoll_event events[MAX_EVENTS] ;
    int nr_events, i;
    for (;;) {

        nr_events = epoll_wait (epfd, events, MAX_EVENTS, -1);
        if (nr_events < 0) {
              log_message(env, ERROR, "Error occured in epoll_wait");
              return -1;
        }

        for (i = 0; i < nr_events; i++) {

            char *cgroup_id = (char *) events[i].data.ptr;

            log_message(env, DEBUG, "OOM occured for cgroup: %s", cgroup_id);

            int notifications_queue_size = (*env)->CallObjectMethod(env, notifications, queue_size);

            //limit the number of messages enqueued
            if (notifications_queue_size > MAX_EVENTS) {
                //remove oldest notification
                (*env)->CallObjectMethod(env, notifications, queue_poll);
                //add notification
                (*env)->CallObjectMethod(env, notifications, queue_add, (*env)->NewStringUTF(env, cgroup_id));
            } else {
                //add notification
                (*env)->CallObjectMethod(env, notifications, queue_add, (*env)->NewStringUTF(env, cgroup_id));
            }

            //cleanup
            //don't want to free since the same cgroup can oom twice if the first oom didn't actually bring down the worker
            //free(cgroup_id);
        }
    }
    printf ("about to exit\n");
    return -1;
}

/*
 * adding and removing cgroups for OOM monitoring
 */
JNIEXPORT jint JNICALL Java_org_apache_storm_container_cgroup_monitor_CgroupOOMMonitor_registerCgroupNative
(JNIEnv *env,
    jobject thisObj,
    jobject cgroups_to_add,
    jobject cgroups_to_remove,
    jobject cgroups_being_monitored) {

    // adding cgroups to OOM monitoring
    while (!(*env)->CallBooleanMethod(env, cgroups_to_add, queue_isEmpty)) {
        jobject CgroupMonitoringInfo_object = (*env)->CallObjectMethod(env, cgroups_to_add, queue_poll);
        jstring cgroup_id_jstring = (jstring) (*env)->GetObjectField(env, CgroupMonitoringInfo_object, CgroupMonitoringInfo_cgroupId);
        char *cgroup_id = get_c_string(env, cgroup_id_jstring);
        log_message(env, INFO, "Attempting to add cgroup: %s for OOM monitoring...", cgroup_id);

        jstring cgroup_memory_oom_control_file_path_jstring = (*env)->CallObjectMethod(env,
            CgroupMonitoringInfo_object, CgroupMonitoringInfo_getFullPathToOOMControlFile);
        char *cgroup_memory_oom_control_file_path = get_c_string(env, cgroup_memory_oom_control_file_path_jstring);

        jstring cgroup_event_control_file_path_jstring = (*env)->CallObjectMethod(env,
            CgroupMonitoringInfo_object, CgroupMonitoringInfo_getFullPathToCgroupEventControlFile);
        char *cgroup_event_control_file_path = get_c_string(env, cgroup_event_control_file_path_jstring);

        int ofd, efd;
        log_message(env, INFO, "Registering cgroup %s with cgroup.event_control path: %s and memory.oom_control path: %s",
            cgroup_id, cgroup_event_control_file_path, cgroup_memory_oom_control_file_path);

        if (register_cgroup(env, cgroup_event_control_file_path, cgroup_memory_oom_control_file_path, &ofd, &efd) == 0) {
            struct epoll_event event;
            event.events = EPOLLIN | EPOLLET;
            // making copy of string so that we can deallocate the c string derived from jstring later in the function
            char *cgroup_id_cpy = (char *) malloc(strlen(cgroup_id) * sizeof(char) +1);
            if (cgroup_id_cpy == NULL) {
                log_message(env, ERROR, "Failed to malloc: Out of Memory!");
                close(ofd);
                close (efd);
                //cleanup
                (*env)->ReleaseStringUTFChars(env, cgroup_id_jstring, cgroup_id);
                (*env)->ReleaseStringUTFChars(env, cgroup_memory_oom_control_file_path_jstring, cgroup_memory_oom_control_file_path);
                (*env)->ReleaseStringUTFChars(env, cgroup_event_control_file_path_jstring, cgroup_event_control_file_path);
                return -1;
            }
            strcpy(cgroup_id_cpy, cgroup_id);
            event.data.ptr = cgroup_id_cpy;
            if (epoll_ctl (epfd, EPOLL_CTL_ADD, efd, &event) != 0) {
                log_message(env, ERROR, "Not successful in adding event to be monitored by epoll with error %s", strerror(errno));
                close(ofd);
                close (efd);
                //cleanup
                (*env)->ReleaseStringUTFChars(env, cgroup_id_jstring, cgroup_id);
                (*env)->ReleaseStringUTFChars(env, cgroup_memory_oom_control_file_path_jstring, cgroup_memory_oom_control_file_path);
                (*env)->ReleaseStringUTFChars(env, cgroup_event_control_file_path_jstring, cgroup_event_control_file_path);
                return -1;
            }
            (*env)->CallObjectMethod(env, CgroupMonitoringInfo_object, CgroupMonitoringInfo_setEfd, efd);
            (*env)->CallObjectMethod(env, CgroupMonitoringInfo_object, CgroupMonitoringInfo_setOfd, ofd);
            (*env)->CallObjectMethod(env, cgroups_being_monitored, map_put, (*env)->NewStringUTF(env, cgroup_id), CgroupMonitoringInfo_object);
            //store the event.data.ptr so we can free it after the cgroup needs to be removed from monitoring
            (*env)->CallObjectMethod(env, CgroupMonitoringInfo_object, CgroupMonitoringInfo_setEventDataPtr, event.data.ptr);

            log_message(env, INFO, "Successful in adding cgroup %s with eventfd: %i OOM control fd: %i to epoll fd: %i for OOM monitoring", cgroup_id, efd, ofd, epfd);
        } else {
            log_message(env, ERROR, "cgroup %s not successfully registered for monitoring!", cgroup_id);
        }
        //cleanup
        (*env)->ReleaseStringUTFChars(env, cgroup_id_jstring, cgroup_id);
        (*env)->ReleaseStringUTFChars(env, cgroup_memory_oom_control_file_path_jstring, cgroup_memory_oom_control_file_path);
        (*env)->ReleaseStringUTFChars(env, cgroup_event_control_file_path_jstring, cgroup_event_control_file_path);
    }

    // removing cgroups from OOM monitoring
    while (!(*env)->CallBooleanMethod(env, cgroups_to_remove, queue_isEmpty)) {
        jobject CgroupMonitoringInfo_object = (*env)->CallObjectMethod(env, cgroups_to_remove, queue_poll);
        jstring cgroup_id_jstring = (jstring) (*env)->GetObjectField(env, CgroupMonitoringInfo_object, CgroupMonitoringInfo_cgroupId);
        char *cgroup_id = get_c_string(env, cgroup_id_jstring);
        log_message(env, INFO, "Attempting to remove cgroup: %s from OOM monitoring...", cgroup_id);

        int ofd = (*env)->CallIntMethod(env, CgroupMonitoringInfo_object, CgroupMonitoringInfo_getOfd);
        int efd = (*env)->CallIntMethod(env, CgroupMonitoringInfo_object, CgroupMonitoringInfo_getEfd);
        log_message(env, INFO, "Unregistering cgroup %s from epoll with fd: %i...", cgroup_id, epfd);

        struct epoll_event event;
        if (epoll_ctl (epfd, EPOLL_CTL_DEL, efd, &event) != 0) {
            log_message(env, ERROR, "Failed to remove event from epoll monitoring with error %s", strerror(errno));
            //clean up
            (*env)->ReleaseStringUTFChars(env, cgroup_id_jstring, cgroup_id);
            return -1;
        }
        //free pointer for cgroupId stored in epoll event.data.ptr
        void * event_data_ptr = (*env)->CallObjectMethod(env, CgroupMonitoringInfo_object, CgroupMonitoringInfo_getEventDataPtr);
        free(event_data_ptr);

        (*env)->CallObjectMethod(env, cgroups_being_monitored, map_remove, (*env)->NewStringUTF(env, cgroup_id));
        log_message(env, INFO, "Successful in removing cgroup %s for OOM monitoring and closing eventfd: %i OOM control fd: %i",cgroup_id, efd, ofd);
        //clean up
        (*env)->ReleaseStringUTFChars(env, cgroup_id_jstring, cgroup_id);
        close(ofd);
        close (efd);
    }
    return 0;
}

/*
 * cleaning up cached objects
 */
 void JNI_OnUnload(JavaVM *vm, void *reserved) {

     JNIEnv *env;
     if ((*vm)->GetEnv(vm, (void **) &env, JNI_VERSION_1_6) != JNI_OK) {
        return;
     }
    log_message(env, INFO, "Cleaning up OOM monitoring native...");
    // delete global class id references
    (*env)->DeleteGlobalRef(env, queue_class);
    (*env)->DeleteGlobalRef(env, map_class);
    (*env)->DeleteGlobalRef(env, CgroupMonitoringInfo_class);
    (*env)->DeleteGlobalRef(env, LoggerFactory_class);
    (*env)->DeleteGlobalRef(env, Logger_class);
    // delete global queue method id references
    (*env)->DeleteGlobalRef(env, queue_poll);
    (*env)->DeleteGlobalRef(env, queue_size);
    (*env)->DeleteGlobalRef(env, queue_isEmpty);
    (*env)->DeleteGlobalRef(env, queue_add);
    // delete global map method id references
    (*env)->DeleteGlobalRef(env, map_put);
    (*env)->DeleteGlobalRef(env, map_remove);
    // delete global CgroupMonitoringInfo method id references
    (*env)->DeleteGlobalRef(env, CgroupMonitoringInfo_getFullPathToOOMControlFile);
    (*env)->DeleteGlobalRef(env, CgroupMonitoringInfo_getFullPathToCgroupEventControlFile);
    (*env)->DeleteGlobalRef(env, CgroupMonitoringInfo_setEfd);
    (*env)->DeleteGlobalRef(env, CgroupMonitoringInfo_setOfd);
    (*env)->DeleteGlobalRef(env, CgroupMonitoringInfo_getEfd);
    (*env)->DeleteGlobalRef(env, CgroupMonitoringInfo_getOfd);
    // delete global logger method id references
    (*env)->DeleteGlobalRef(env, Logger_info);
    (*env)->DeleteGlobalRef(env, Logger_debug);
    (*env)->DeleteGlobalRef(env, Logger_error);
    // delete LOGGER object
    (*env)->DeleteWeakGlobalRef(env, LOGGER);
    //close epfd
    close(epfd);
}

/*
 * registering a cgroup for monitoring
 */
int register_cgroup(JNIEnv *env, char *event_control_path, char *memory_oom_control, int *ofd, int *efd) {
    char buf[BUFSIZE];
    int cfd, wb;
    if (((*efd) = eventfd(0, EFD_NONBLOCK)) == -1) {
        log_message(env, ERROR, "Couldn't get eventfd file descriptor with error: %s", strerror(errno));
        return -1;
    }

    if ((cfd = open(event_control_path, O_WRONLY)) == -1) {
        log_message(env, ERROR, "Couldn't get cgroup.event_control file descriptor with error: %s", strerror(errno));
        close (*efd);
        return -1;
    }

    if (((*ofd) = open(memory_oom_control, O_RDONLY)) == -1) {
        log_message(env, ERROR, "Couldn't get memory.oom_control file descriptor with error: %s", strerror(errno));
        close (*efd);
        close (cfd);
        return -1;
    }

    if ((wb = snprintf(buf, BUFSIZE, "%d %d", (*efd), (*ofd))) >= BUFSIZE) {
        log_message(env, ERROR, "Couldn't write to file cgroup.event_control with error: %s", strerror(errno));
        close (*efd);
        close (cfd);
        close (*ofd);
        return -1;
    }

    if (write(cfd, buf, wb) == -1) {
        log_message(env, ERROR, "Couldn't write to file cgroup.event_control with error: %s", strerror(errno));
        close (*efd);
        close (cfd);
        close (*ofd);
        return -1;
    }

    if (close(cfd) == -1) {
        log_message(env, ERROR, "Couldn't close file cgroup.event_control with error: %s", strerror(errno));
        close (*efd);
        close (cfd);
        close (*ofd);
        return -1;
    }
    return 0;
}

/*
 * get c string from jstring object
 */
char *get_c_string(JNIEnv *env, jstring jstr) {
    char *ret = NULL;
    if (jstr != NULL) {
        ret = (char *)(*env)->GetStringUTFChars(env, jstr, NULL);
    }
    return ret;
}

/*
 * Log messages
 */
int log_message(JNIEnv *env, int type, char *format, ...) {

    char buf[BUFSIZE];
    int ret;
    va_list valist;
    va_start(valist, format);

    ret = vsprintf(buf, format, valist);

    va_end(valist);

    if (ret < 0) {
        printf("Unsuccessful in parsing string for logging\n");
        return -1;
    }

    switch (type) {
        case INFO :
            (*env)->CallObjectMethod(env, LOGGER, Logger_info, (*env)->NewStringUTF(env, buf));
            break;
        case ERROR :
            (*env)->CallObjectMethod(env, LOGGER, Logger_error, (*env)->NewStringUTF(env, buf));
            break;
        case DEBUG :
            (*env)->CallObjectMethod(env, LOGGER, Logger_debug, (*env)->NewStringUTF(env, buf));
            break;
        default:
            printf ("Unknown logger level!\n");
            return -1;
    }

    if ((*env)->ExceptionCheck(env)) {
           printf("Failed to call to log message\n");
           return -1;
    }
    return 0;
}
