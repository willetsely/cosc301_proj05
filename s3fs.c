/* This code is based on the fine code written by Joseph Pfeiffer for his
   fuse system tutorial. */

#include "s3fs.h"
#include "libs3_wrapper.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/xattr.h>

#define GET_PRIVATE_DATA ((s3context_t *) fuse_get_context()->private_data)
#define ENTRY_SIZE (sizeof(entry_t))

/*
 * For each function below, if you need to return an error,
 * read the appropriate man page for the call and see what
 * error codes make sense for the type of failure you want
 * to convey.  For example, many of the calls below return
 * -EIO (an I/O error), since there are no S3 calls yet
 * implemented.  (Note that you need to return the negative
 * value for an error code.)
 */

/* *************************************** */
/*        Stage 1 callbacks                */
/* *************************************** */

/*
 * Initialize the file system.  This is called once upon
 * file system startup.
 */
void *fs_init(struct fuse_conn_info *conn)
{
    fprintf(stderr, "fs_init --- initializing file system.\n");
    s3context_t *ctx = GET_PRIVATE_DATA;
    s3fs_clear_bucket(ctx->s3bucket);
    
    ssize_t success = 0;
    const char *key = "/"
    time_t curr_time = time(NULL);
    entry_t *root = (entry_t *)malloc(ENTRY_SIZE);
    
    root->type = 'd';
    strncpy(root->name, ".", 1);
    root->mode = (S_IFDIR | S_IRUSR | S_IWUSR | S_IXUSR);
    root->links = 1;
    root->uid = getuid();
    root->gid = getgid();
    root->curr_time;
    root->curr_time;
    root->curr_time;
    
    ssize_t success = s3fs_put_object(ctx->s3bucket, key, (uint8_t *)root, ENTRY_SIZE);       
    free(root);
    if (success == -1)
        return -EIO;
    return ctx;
}

/*
 * Clean up filesystem -- free any allocated data.
 * Called once on filesystem exit.
 */
void fs_destroy(void *userdata) {
    fprintf(stderr, "fs_destroy --- shutting down file system.\n");
    free(userdata);
}


/* 
 * Get file attributes.  Similar to the stat() call
 * (and uses the same structure).  The st_dev, st_blksize,
 * and st_ino fields are ignored in the struct (and 
 * do not need to be filled in).
 */

int fs_getattr(const char *path, struct stat *statbuf) {
    fprintf(stderr, "fs_getattr(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
    
    char *path_name = dirname(strdup(path));
    char *base_name = basename(strdup(path));

    uint8_t *buffer = NULL;
    ssize_t success = 0;
    
    success = s3fs_get_object(ctx->s3bucket, path_name, &buffer, 0, 0);
    if(success < 0)
    {
        fprint(stderr, "directory %s does not exist\n", path_name);
        return -EIO;
    }
    
    int num_entries = sizeof(buffer)/ENTRY_SIZE;
    entry_t * curr_dir = (entry_t *) buffer;
    free(buffer);
    
    int i = 0;
    for(; i < num_entries; i++)
    {
        if(0 == strncmp(curr_dir[i].name, base_name, 256))
        {
            if(curr_dir[i].type == 'f')
            {       
				statbuf->st_dev = 0;
				statbuf->st_ino = 0;
                statbuf->st_mode = curr_dir[i].mode;
                statbuf->st_nlink = curr_dir[i].links;
                statbuf->st_uid = curr_dir[i].uid;
                statbuf->st_gid = curr_dir[i].gid;
                statbuf->st_rdev = 0;
                statbuf->st_size = curr_dir[i].size;
				statbuf->st_blksize = 0;
				statbuf->st_blocks = 0;
                statbuf->st_atime = curr_dir[i].atime;
                statbuf->st_mtime = curr_dir[i].mtime;
                statbuf->st_ctime = curr_dir[i].ctime;
                return 0; //success!
            }
            else if(curr_dir[i].type == 'd')
            {
                success = s3fs_get_object(ctx->s3bucket, base_name, &buffer, 0, ENTRY_SIZE);
                if(success < 0)
                {
                    fprint(stderr, "directory %s does not exist\n", base_name);
                    return -EIO;
                }

                entry_t dot = (entry_t) buffer;
                free(buffer);

                statbuf->st_dev = 0;
				statbuf->st_ino = 0;
                statbuf->st_mode = dot.mode;
                statbuf->st_nlink = dot.links;
                statbuf->st_uid = dot.uid;
                statbuf->st_gid = dot.gid;
                statbuf->st_rdev = 0;
                statbuf->st_size = dot.size;
				statbuf->st_blksize = 0;
				statbuf->st_blocks = 0;
                statbuf->st_atime = dot.atime;
                statbuf->st_mtime = dot.mtime;
                statbuf->st_ctime = dot.ctime;
                return 0; //success!
            }
        } 
    } 
    return -EIO;
}

/*
 * Open directory
 *
 * This method should check if the open operation is permitted for
 * this directory
 */
int fs_opendir(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_opendir(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
    uint8_t *buffer = NULL;
    ssize_t success = 0;
    success = s3fs_get_object(ctx->s3bucket, path, &buffer, 0, 0);
    free(buffer);
    if (success < 0)
    	return -EIO;
	return 0;
}


/*
 * Read directory.  See the project description for how to use the filler
 * function for filling in directory items.
 */
int fs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
         struct fuse_file_info *fi)
{
    fprintf(stderr, "fs_readdir(path=\"%s\", buf=%p, offset=%d)\n",
          path, buf, (int)offset);
    s3context_t *ctx = GET_PRIVATE_DATA;
    uint8_t *buffer = NULL;
    ssize_t success = 0;
    ssize_t byte_count = (ssize_t)offset;
    success = s3fs_get_object(ctx->s3bucket, path, &buffer, 0, byte_count);
    if (success < 0)
    {
        free(buffer);
        return -EIO;
    }
	int num_entries = (int)success / sizeof(entry_t);
	int i = 0;
	entry_t *entries = (entry_t *)buffer;
	for (; i < num_entries; i++)
	{
		if (filler(buf, entries[i].name, NULL, 0) != 0)
		{
			free(buffer);
			return -ENOMEM;
		}
	}
	free(buffer);
	return 0; 
}


/*
 * Release directory.
 */
int fs_releasedir(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_releasedir(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
    uint8_t *buffer = NULL;
    ssize_t success = 0;
    success = s3fs_get_object(ctx->s3bucket, path, &buffer, 0, 0);
    free(buffer);    
    if (success < 0)
        return -EIO;
    return success;
}


/* 
 * Create a new directory.
 *
 * Note that the mode argument may not have the type specification
 * bits set, i.e. S_ISDIR(mode) can be false.  To obtain the
 * correct directory type bits (for setting in the metadata)
 * use mode|S_IFDIR.
 */
int fs_mkdir(const char *path, mode_t mode) {
    fprintf(stderr, "fs_mkdir(path=\"%s\", mode=0%3o)\n", path, mode);
    s3context_t *ctx = GET_PRIVATE_DATA;
    mode |= S_IFDIR;
    
    uint8_t *buffer = NULL;
    ssize_t success = s3fs_get_object(ctx->s3bucket, path, &buffer, 0, 0);
    if(success >=  0)
    {
		free(buffer);
        return -EEXIST;
    } 
	free(buffer);

    char *path_name = dirname(strdup(path));
    char *base_name = basename(strdup(path));
    
    success = s3fs_get_object(ctx->s3bucket, path_name, &buffer, 0, 0);
	int num_entries = sizeof(buffer)/ENTRY_SIZE;
	entry_t * new_parent = (entry *)malloc(sizeof(buffer) + ENTRY_SIZE);
	entry_t *old_parent = (entry_t *)buffer;
	free(buffer);

	int i = 0;
	for(; i < num_entries; i++)
	{
		//copy old entries over
		new_parent[i] = old_parent[i];
    }
	//reset size of parent directory
    new_parent[0].size = (num_entries + 1)*ENTRY_SIZE;
	//create new entry
	strncpy(new_parent[i].name, base_name, 256);
	new_parent[i].type = 'd';
	
	success = s3fs_put_object(ctx->s3bucket, path_name, new_parent, sizeof(new_parent));
	if(success < 0)
	{
		return -EIO;
	}
	time_t curr_time = time(NULL);	
	entry_t *new_dir = (entry_t *)malloc(ENTRY_SIZE);
	strncpy(new_dir[0].name, ".", 256);
	new_dir[0].type = 'd';
	new_dir[0].mode = (S_IFDIR | S_IRUSR | S_IWUSR | S_IXUSR);
    new_dir[0].links = 1;
    new_dir[0].uid = getuid();
    new_dir[0].gid = getgid();
    new_dir[0].size =  ENTRY_SIZE;
    new_dir[0].atime = curr_time;
    new_dir[0].mtime = curr_time;
    new_dir[0].ctime = curr_time;

    //return success
    return 0;
}


/*
 * Remove a directory. 
 */
int fs_rmdir(const char *path) {
    fprintf(stderr, "fs_rmdir(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
    uint8_t *buffer = NULL;
    ssize_t success = 0;
    success = s3fs_get_object(ctx->s3bucket, path, &buffer, 0, 0);
    char *path_name = NULL;
    char *base_name = NULL;
    if (success < 0)
    {
        free(buffer);
        return -EIO;
    }
    if (success != ENTRY_SIZE)     //directory not empty
    {
        printf(stderr, "fs_rmdir(path=\"%s\" is not empty)\n", path);
        free(buffer);
        return -EIO;
    }
    else
    {
        free(buffer);
        if(s3fs_remove_object(ctx->s3bucket, path) == 0)
        {
            printf("%s Directory was successfully removed\n", path);
            path_name = dirname(strdup(path));
            base_name = basename(strdup(path));
            ssize_t parent_size = 0;
            uint8_t *buff = NULL;
            parent_size = s3fs_get_object(ctx->s3bucket, path_name, &buff, 0, 0);
            entry_t *parent_entry = (entry_t *)buff;
            int num_entries = (int)parent_size / (int)ENTRY_SIZE;
            entry_t *new_parent = (entry_t *)malloc((int)ENTRY_SIZE * (num_entries - 1));
            int j = 1;
            int i = 0;
            for (; i < num_entries; i++)
            {
                if (strcmp(parent_entry[i].name, base_name) == 0)
                {
                    if (j == num_entries)
                        break;
                    else
                    {
                        while (j < num_entries)
                        {
                            new_parent[i] = parent_entry[j]; //copying next index of old parent to current
                            i++;                             //index of new parent
                            j++;
                        }
                        break;
                    }
                }
                new_parent[i] = parent_entry[i];  //copies old parent entry to new parent entry
                j++;
            }
            ssize_t overwrite = 0;
            overwrite = s3fs_put_object(ctx->s3bucket, path_name, &buff, (num_entries - 1));
            free(buff);   
            return 0;
        }
        return -EIO;
    }
}


/* *************************************** */
/*        Stage 2 callbacks                */
/* *************************************** */


/* 
 * Create a file "node".  When a new file is created, this
 * function will get called.  
 * This is called for creation of all non-directory, non-symlink
 * nodes.  You *only* need to handle creation of regular
 * files here.  (See the man page for mknod (2).)
 */
int fs_mknod(const char *path, mode_t mode, dev_t dev) {
    fprintf(stderr, "fs_mknod(path=\"%s\", mode=0%3o)\n", path, mode);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}


/* 
 * File open operation
 * No creation, or truncation flags (O_CREAT, O_EXCL, O_TRUNC)
 * will be passed to open().  Open should check if the operation
 * is permitted for the given flags.  
 * 
 * Optionally open may also return an arbitrary filehandle in the 
 * fuse_file_info structure (fi->fh).
 * which will be passed to all file operations.
 * (In stages 1 and 2, you are advised to keep this function very,
 * very simple.)
 */
int fs_open(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_open(path\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}


/* 
 * Read data from an open file
 *
 * Read should return exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  
 */
int fs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_read(path=\"%s\", buf=%p, size=%d, offset=%d)\n",
          path, buf, (int)size, (int)offset);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}


/*
 * Write data to an open file
 *
 * Write should return exactly the number of bytes requested
 * except on error.
 */
int fs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_write(path=\"%s\", buf=%p, size=%d, offset=%d)\n",
          path, buf, (int)size, (int)offset);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}


/*
 * Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.  
 *
 * For every open() call there will be exactly one release() call
 * with the same flags and file descriptor.  It is possible to
 * have a file opened more than once, in which case only the last
 * release will mean, that no more reads/writes will happen on the
 * file.  The return value of release is ignored.
 */
int fs_release(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_release(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}


/*
 * Rename a file.
 */
int fs_rename(const char *path, const char *newpath) {
    fprintf(stderr, "fs_rename(fpath=\"%s\", newpath=\"%s\")\n", path, newpath);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}


/*
 * Remove a file.
 */
int fs_unlink(const char *path) {
    fprintf(stderr, "fs_unlink(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}
/*
 * Change the size of a file.
 */
int fs_truncate(const char *path, off_t newsize) {
    fprintf(stderr, "fs_truncate(path=\"%s\", newsize=%d)\n", path, (int)newsize);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}


/*
 * Change the size of an open file.  Very similar to fs_truncate (and,
 * depending on your implementation), you could possibly treat it the
 * same as fs_truncate.
 */
int fs_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_ftruncate(path=\"%s\", offset=%d)\n", path, (int)offset);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}


/*
 * Check file access permissions.  For now, just return 0 (success!)
 * Later, actually check permissions (don't bother initially).
 */
int fs_access(const char *path, int mask) {
    fprintf(stderr, "fs_access(path=\"%s\", mask=0%o)\n", path, mask);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return 0;
}


/*
 * The struct that contains pointers to all our callback
 * functions.  Those that are currently NULL aren't 
 * intended to be implemented in this project.
 */
struct fuse_operations s3fs_ops = {
  .getattr     = fs_getattr,    // get file attributes
  .readlink    = NULL,          // read a symbolic link
  .getdir      = NULL,          // deprecated function
  .mknod       = fs_mknod,      // create a file
  .mkdir       = fs_mkdir,      // create a directory
  .unlink      = fs_unlink,     // remove/unlink a file
  .rmdir       = fs_rmdir,      // remove a directory
  .symlink     = NULL,          // create a symbolic link
  .rename      = fs_rename,     // rename a file
  .link        = NULL,          // we don't support hard links
  .chmod       = NULL,          // change mode bits: not implemented
  .chown       = NULL,          // change ownership: not implemented
  .truncate    = fs_truncate,   // truncate a file's size
  .utime       = NULL,          // update stat times for a file: not implemented
  .open        = fs_open,       // open a file
  .read        = fs_read,       // read contents from an open file
  .write       = fs_write,      // write contents to an open file
  .statfs      = NULL,          // file sys stat: not implemented
  .flush       = NULL,          // flush file to stable storage: not implemented
  .release     = fs_release,    // release/close file
  .fsync       = NULL,          // sync file to disk: not implemented
  .setxattr    = NULL,          // not implemented
  .getxattr    = NULL,          // not implemented
  .listxattr   = NULL,          // not implemented
  .removexattr = NULL,          // not implemented
  .opendir     = fs_opendir,    // open directory entry
  .readdir     = fs_readdir,    // read directory entry
  .releasedir  = fs_releasedir, // release/close directory
  .fsyncdir    = NULL,          // sync dirent to disk: not implemented
  .init        = fs_init,       // initialize filesystem
  .destroy     = fs_destroy,    // cleanup/destroy filesystem
  .access      = fs_access,     // check access permissions for a file
  .create      = NULL,          // not implemented
  .ftruncate   = fs_ftruncate,  // truncate the file
  .fgetattr    = NULL           // not implemented
};



/* 
 * You shouldn't need to change anything here.  If you need to
 * add more items to the filesystem context object (which currently
 * only has the S3 bucket name), you might want to initialize that
 * here (but you could also reasonably do that in fs_init).
 */
int main(int argc, char *argv[]) {
    // don't allow anything to continue if we're running as root.  bad stuff.
    if ((getuid() == 0) || (geteuid() == 0)) {
    	fprintf(stderr, "Don't run this as root.\n");
    	return -1;
    }
    s3context_t *stateinfo = malloc(sizeof(s3context_t));
    memset(stateinfo, 0, sizeof(s3context_t));

    char *s3key = getenv(S3ACCESSKEY);
    if (!s3key) {
        fprintf(stderr, "%s environment variable must be defined\n", S3ACCESSKEY);
        return -1;
    }
    char *s3secret = getenv(S3SECRETKEY);
    if (!s3secret) {
        fprintf(stderr, "%s environment variable must be defined\n", S3SECRETKEY);
        return -1;
    }
    char *s3bucket = getenv(S3BUCKET);
    if (!s3bucket) {
        fprintf(stderr, "%s environment variable must be defined\n", S3BUCKET);
        return -1;
    }
    strncpy((*stateinfo).s3bucket, s3bucket, BUFFERSIZE);

    fprintf(stderr, "Initializing s3 credentials\n");
    s3fs_init_credentials(s3key, s3secret);

    fprintf(stderr, "Totally clearing s3 bucket\n");
    s3fs_clear_bucket(s3bucket);

    fprintf(stderr, "Starting up FUSE file system.\n");
    int fuse_stat = fuse_main(argc, argv, &s3fs_ops, stateinfo);
    fprintf(stderr, "Startup function (fuse_main) returned %d\n", fuse_stat);
    
    return fuse_stat;
}
