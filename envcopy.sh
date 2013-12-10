#!/bin/bash 

#
# This file contains your Amazon S3 credentials for use
# in project 5.  Save it in your project 5 source code
# folder.  **Don't** check it in to git.  Guard it
# carefully.
#

mkdir ./mnt
export LD_LIBRARY_PATH=`pwd`/libs3-2.0/build/lib
export S3_ACCESS_KEY_ID="AKIAI5KULN43DOFZCZWA"
export S3_SECRET_ACCESS_KEY="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"
export S3_BUCKET="edu.colgate.cosc301.rely"
