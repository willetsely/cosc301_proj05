#ifndef __USERSPACEFS_H__
#define __USERSPACEFS_H__

#include <sys/stat.h>
#include <stdint.h>   // for uint32_t, etc.
#include <sys/time.h> // for struct timeval

/* This code is based on the fine code written by Joseph Pfeiffer for his
   fuse system tutorial. */

/* Declare to the FUSE API which version we're willing to speak */
#define FUSE_USE_VERSION 26

#define S3ACCESSKEY "S3_ACCESS_KEY_ID"
#define S3SECRETKEY "S3_SECRET_ACCESS_KEY"
#define S3BUCKET "S3_BUCKET"

#define BUFFERSIZE 1024

// store filesystem state information in this struct
typedef struct {
    char s3bucket[BUFFERSIZE];
} s3context_t;

/*
 * Other data type definitions (e.g., a directory entry
 * type) should go here.
 */

typedef struct {
    char type;
    char[256] name;

    //metadata
    mode_t mode;    //protection
    nlink_t links;  //hard links
    uid_t uid;      //user ID of owner
    gid_t gid;      //group ID of owner
    off_t size;     //size in bytes
    time_t atime;   //time of last access
    time_t mtime;   //time of last modification
    time_t ctime;   //time of last status change
} entry_t;

#endif // __USERSPACEFS_H__
