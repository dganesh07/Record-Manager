#include "dberror.h"
#include "storage_mgr.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
FILE *filePtr;
void initStorageManager(void) {
	filePtr = NULL;
}

extern RC createPageFile(char *fileName) {

    FILE *filePtr;
    char initialSize[PAGE_SIZE] = {'\0'};
    filePtr = fopen(fileName, "w+");

    if (filePtr == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    fwrite(initialSize, sizeof (char), PAGE_SIZE, filePtr);
    fclose(filePtr);
    return RC_OK;
}

extern RC openPageFile(char *fileName, SM_FileHandle *fHandle) {

    FILE *filePtr;
    filePtr = fopen(fileName, "rb+");
    if (filePtr == NULL) { // If file pointer doesn't point to file , return error
        return RC_FILE_NOT_FOUND;
    }

    fseek(filePtr, 0, SEEK_END); // makes the cursor position to end of the byte
    int noOfBytes = ftell(filePtr); //no of bytes from beg(4096) because the pointer is at the end

    fHandle->totalNumPages = (int)ceil((double) noOfBytes / PAGE_SIZE); // total bytes from beg to end/page size
    fHandle->curPagePos = 0;
    fHandle->mgmtInfo = filePtr;
    fHandle->fileName = fileName;
    return RC_OK;
}

extern RC closePageFile(SM_FileHandle *fHandle) {

    fclose(fHandle->mgmtInfo);
    if (fHandle->mgmtInfo == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    return RC_OK;
}

extern RC destroyPageFile(char *fileName) {
    if (remove(fileName)) {
        return RC_FILE_NOT_FOUND;
    }
    return RC_OK;
}


extern RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {

    //Checks if pageNum value is valid
    if (pageNum > fHandle->totalNumPages || pageNum < 0 || fHandle->totalNumPages < 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    FILE *filePtr = fHandle->mgmtInfo;

    if (filePtr == NULL) { // If file pointer doesn't point to file , return error
        return RC_FILE_NOT_FOUND;
    }

    int seekVal = fseek(filePtr, (pageNum * PAGE_SIZE), SEEK_SET); // Point the pointer to beg of the block

    if (seekVal != 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    fread(memPage, sizeof (char), PAGE_SIZE, filePtr); // read the file page into mempage array
    //fHandle->curPagePos = ceil((double) (ftell(filePtr) / PAGE_SIZE)) - 1; // get No of bytes from beg / Page size will point to current pos
    fHandle->curPagePos = pageNum;
    fHandle->mgmtInfo = filePtr;
    return RC_OK;
}

extern int getBlockPos(SM_FileHandle *fHandle) {
    return fHandle->curPagePos;
}

extern RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(0, fHandle, memPage);
}

extern RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {

    if (fHandle->curPagePos - 1 < 0) {
        return RC_PREVIOUS_BLOCK_DOES_NOT_EXIST;
    }

    return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}


extern RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

extern RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
}

extern RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {

    if (fHandle->totalNumPages - 1 <= 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }
    return readBlock(fHandle->totalNumPages, fHandle, memPage);
}

extern RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {


    FILE *filePtr = fHandle->mgmtInfo;

    if (filePtr == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    if (fHandle->totalNumPages < pageNum || fHandle->totalNumPages < 0 || pageNum < 0) {
        return RC_WRITE_FAILED;
    }

    int successvalue = fseek(filePtr, (PAGE_SIZE * pageNum), SEEK_SET);
    if (successvalue != 0) {
        return RC_FILE_NOT_FOUND;
    }
    fwrite(memPage, sizeof (char), PAGE_SIZE, filePtr);
    fHandle->curPagePos = pageNum;
    fHandle->mgmtInfo = filePtr;
    return RC_OK;
}

extern RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {

    int blckPos = fHandle->curPagePos;
    int val = writeBlock(blckPos, fHandle, memPage);
    if (RC_OK == val)
        return RC_OK;
    else
        return RC_WRITE_FAILED;
}

extern RC appendEmptyBlock(SM_FileHandle *fHandle) {

    FILE *filePtr = fHandle->mgmtInfo;

    if (filePtr == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    fHandle->totalNumPages += 1;
    fHandle->curPagePos = fHandle->totalNumPages - 1;
    fseek(filePtr, 0, SEEK_END); //SEEk_END

    char initialSize[PAGE_SIZE] = {'\0'};

    fwrite(initialSize, sizeof (char), PAGE_SIZE, filePtr);

    fHandle->mgmtInfo = filePtr;
    return RC_OK;
}

extern RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    int count = 0;

    if (fHandle->totalNumPages >= numberOfPages) {
        return RC_OK;
    }

    int diff = numberOfPages - fHandle->totalNumPages;
    int i;
    for (i = 0; i < diff; i++) {
        if (appendEmptyBlock(fHandle) == RC_OK) {
            continue;
        } else {
            count++;
            break;
        }
    }

    if (count > 0) {
        return RC_APPEND_ERROR;
    }
    return RC_OK;

}
