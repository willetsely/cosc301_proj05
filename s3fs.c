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
    
    const char *key = "/";
    time_t curr_time = time(NULL);
    entry_t *root = (entry_t *)malloc(ENTRY_SIZE);
    
    root->type = 'd';
    strncpy(root->name, ".", 1);
    root->mode = (S_IFDIR | S_IRUSR | S_IWUSR | S_IXUSR);
    root->links = 1;
    root->uid = getuid();
    root->gid = getgid();
    root->atime = curr_time;
    root->mtime = curr_time;
    root->ctime = curr_time;
    
    ssize_t success = s3fs_put_object(ctx->s3bucket, key, (uint8_t *)root, ENTRY_SIZE);       
    printf("initialized / \n");
    free(root);
    if (success == -1)
        return -EIO;
    printf("PARTYYYYYYYYYYYY TIME!!!");
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
        printf(stderr, "directory %s does not exist\n", path_name);
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
                    printf(stderr, "directory %s does not exist\n", base_name);
                    return -EIO;
                }

                entry_t *dot = (entry_t *)buffer;
                free(buffer);

                statbuf->st_dev = 0;
				statbuf->st_ino = 0;
                statbuf->st_mode = dot[0].mode;
                statbuf->st_nlink = dot[0].links;
                statbuf->st_uid = dot[0].uid;
                statbuf->st_gid = dot[0].gid;
                statbuf->st_rdev = 0;
                statbuf->st_size = dot[0].size;
				statbuf->st_blksize = 0;
				statbuf->st_blocks = 0;
                statbuf->st_atime = dot[0].atime;
                statbuf->st_mtime = dot[0].mtime;
                statbuf->st_ctime = dot[0].ctime;
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
    fprintf(stderr, "\n******fs_opendir(path=\"%s\")*******\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
        
    char *path_name = dirname(strdup(path));
    char *base_name = basename(strdup(path));    

    uint8_t *buffer = NULL;
    ssize_t success = 0;
    
    success = s3fs_get_object(ctx->s3bucket, path, &buffer, 0, 0);
    if (success < 0)
    {
        free(buffer);
    	return -EIO;
    }
    
    entry_t *entries = (entry_t *)buffer;
    entries[0].atime = curr_time;
    free(buffer);

    ssize_t overwrite = s3fs_put_object(ctx->s3bucket, path, (uint8_t *)entries, success);
	printf("\n*****got through opendir******");
	return 0;
}


/*
 * Read directory.  See the project description for how to use the filler
 * function for filling in directory items.
 */
int fs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
         struct fuse_file_info *fi)
{
    fprintf(stderr, "\n******fs_readdir(path=\"%s\", buf=%p, offset=%d)*******\n",
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
	printf("\n*****got through readdir*****");
	return 0; 
}


/*
 * Release directory.
 */
int fs_releasedir(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "\n******fs_releasedir(path=\"%s\")******\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
    uint8_t *buffer = NULL;
    ssize_t success = 0;
    success = s3fs_get_object(ctx->s3bucket, path, &buffer, 0, 0);
    free(buffer);    
    if (success < 0)
        return -EIO;
	printf("\n******got through releasedir*******\n");
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
	entry_t * new_parent = (entry_t *)malloc(sizeof(buffer) + ENTRY_SIZE);
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
	
	uint8_t *blob_new_parent = (uint8_t *) new_parent;
	free(new_parent);
	success = s3fs_put_object(ctx->s3bucket, path_name, blob_new_parent, sizeof(blob_new_parent));
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
	printf("\n*******got through mkdir******\n");
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
        printf("fs_rmdir(path=\"%s\" is not empty)\n", path);
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
            int new_entry_number = (int)ENTRY_SIZE * (num_entries - 1);
            entry_t *new_parent = (entry_t *)malloc(new_entry_number);
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
            ssize_t overwrite = s3fs_put_object(ctx->s3bucket, path_name, (uint8_t *)new_parent, new_entry_number);
            free(buff);   
            return 0;
        }
        return -EIO;
    }
}

char entry_type(const char *path)
{
	s3context_t *ctx = GET_PRIVATE_DATA;
	
	char ret_val = 'z';
	uint8_t buffer = NULL;	
	ssize_t success = s3fs_get_object(ctx->s3bucket, path, &buffer, 0, 0);
	if(success < 0)
	{
		free(buffer);
		return ret_val;
	}	

	char *path_name = dirname(strdup(path));
	char *base_name = basename(strdup(path));

	success = s3fs_get_object(ctx->s3bucket, path_name, &buffer, 0, 0);
	int num_entries = success/ENTRY_SIZE;
	entry_t *parent_dir = (entry_t *)buffer;
	free(buffer)

	int i = 0;
	for(;i < num_entries; i++)
	{
		if(0 == strncmp(parent_dir[i].name, base_name, 256)
		{		
			ret_val = parent_dir[i].type;
			return ret_val;		
		}	
	}
	return ret_val;
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
	
	uint8_t *buffer = NULL;
	ssize_t success = s3fs_get_object(ctx->s3bucket, path, &buffer, 0, 0);
	if(success >= 0)
		return -EEXIST;
	
	char *path_name = dirname(strdup(path));
	char *file_name = basename(strdup(path));

	success = s3fs_get_object(ctx->s3bucket, path_name, &buffer, 0, 0);
	if(success < 0)
		return -EIO;

	//update the directory
	int num_entries = sizeof(buffer)/ENTRY_SIZE;
	int i = 0;
	entry_t *new_dir = (entry_t *)malloc((num_entries + 1)*ENTRY_SIZE);
	entry_t *old_dir = (entry_t *)buffer;
	free(buffer);
		
	for(;i < num_entries; i++)
	{
		new_dir[i] = old_dir[i];
	}
	new_dir[0].size = ENTRY_SIZE*(num_entries + 1);
	new_dir[0].atime = curr_time;
	new_dir[0].mtime = curr_time;

	strncpy(new_dir[i].name, file_name, 256);
	new_dir[i].type = 'f';
	new_dir[i].mode = mode;
	new_dir[i].links = 1;
	new_dir[i].uid = getuid();
	new_dir[i].gid = getgid();
	new_dir[i].size = 0;
	new_dir[i].atime = curr_time;
	new_dir[i].mtime = curr_time;
	new_dir[i].ctime = curr_time;

	uint8_t *blob_new_dir = (uint8_t *) new_dir;
	free(new_dir);
	success = s3fs_put_object(ctx->s3bucket, path_name, blob_new_dir, sizeof(blob_new_dir));
	if(success < 0)
		return -EIO;
 
	//s3 the file
	success = s3fs_put_object(ctx->s3bucket, file_name, NULL, 0);
	if(success < 0)
    	return -EIO;
	return 0;
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
    uint8_t *buffer = NULL;
    ssize_t success = s3fs_get_object(ctx->s3bucket, path, &buffer, 0, 0);
    if (success < 0)
    {
        free(buffer);   
        return -EIO;
    }
    entry_t *entry = (entry_t *)buffer;
    entry[0].a_time = curr_time;
    ssize_t overwrite = s3fs_put_object(ctx->s3bucket, path, entry, success);
    free(buffer);
    if (overwrite < 0)
        return -EIO;
    return 0;   //file is in the bucket! yay!
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
    ssize_t success = s3fs_get_object(ctx->s3bucket, path, &buf, (ssize_t)offset, size);
    if (success < 0)
        return -EIO;
    return success;
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
    uint8_t *buffer = NULL;
    ssize_t success = s3fs_get_object(ctx->s3bucket, path, &buffer, 0, 0);
    free(buffer);
    if (success < 0)
        return -EIO;
    return (int)success;
}


/*
 * Rename a file.
 */
int fs_rename(const char *path, const char *newpath) {
    fprintf(stderr, "fs_rename(fpath=\"%s\", newpath=\"%s\")\n", path, newpath);
    s3context_t *ctx = GET_PRIVATE_DATA;
    uint8_t *buffer = NULL;
    char *path_name = dirname(strdup(path));
    char *base_name = basename(strdup(path));
    char *new_basename = basename(strdup(newpath));
    ssize_t got_obj = s3fs_get_object(ctx->s3bucket, path, &buffer, 0, 0);
    if (got_obj < 0)
    {
        free(buffer);
        return -EIO;        //did not get file
    }
    int removed = s3fs_remove_object(ctx->s3bucket, path);
    if (removed < 0)
    {
        free(buffer);
        return -EIO;       //error removing the file
    }   
    ssize_t success = s3fs_put_object(ctx->s3bucket, newpath, buffer, got_obj);
    if (success < 0)
    {
        free(buffer);
        return -EIO;        //error putting in object with new pathname
    }
    free(buffer);

    //update parent directory
    uint8_t *parent_buffer = NULL;
    ssize_t parent_success = s3fs_get_object(ctx->s3bucket, path_name, &parent_buffer, 0, 0);
    if (parent_success < 0)
    {
        free(parent_buffer);
        return -EIO;  
    }  
    int num_entries = (int)parent_success / ENTRY_SIZE;
    entry_t *parent = (entry_t *)parent_buffer;
    int i = 0;
    for (; i < num_entries; i++)
    {
        if (strcmp(parent[i].name, base_name) == 0)
        {
            strncpy(parent[i].name, new_basename, 256);
            free(parent_buffer);
            return 0;
        }
    }
    ssize_t overwrite = s3fs_put_object(ctx->s3bucket, path_name, parent, parent_success);
    free(parent_buffer);
    return -EIO;
}


/*
 * Remove a file.
 */
int fs_unlink(const char *path) {
    fprintf(stderr, "fs_unlink(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
    char *parent_path = dirname(strdup(path));
    char *file_name = basename(strdup(path));
    
    //remove file
    int removed = s3fs_remove_object(ctx->s3bucket, path);
    if (removed < 0)
        return -EIO;    //error while trying to remove file
    
    //update parent directory
    uint8_t *buffer = NULL;
    ssize_t parent_size = s3fs_get_object(ctx->s3bucket, parent_path, &buffer, 0, 0);
    if (parent_size < 0)
    {
        free(buffer);
        return -EIO;
    }
    int num_entries = (int)parent_size / (int)ENTRY_SIZE;
    int new_parent_size = (int)ENTRY_SIZE * (num_entries - 1);
    entry_t *parent = (entry_t *)buffer;
    entry_t *new_parent = (entry_t *)malloc(new_parent_size);
    int i = 0;
    int j = 1;
    for (; i < num_entries; i++)
    {
        if (strcmp(parent[i].name, file_name) == 0)
        {
            if (j == num_entries)
                break;
            while (j < num_entries)
            {
                new_parent[i] = parent[j];
                j++;
                i++;
            }
            break;
        }
        new_parent[i] = parent[i];
        j++;
    }
    ssize_t overwrite = s3fs_put_object(ctx->s3bucket, parent_path, (uint8_t *)new_parent, new_parent_size);
    free(buffer);
    return 0;
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
