#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>
#include "replacementStrategies.c"


/**********************************************************************************
* Function Name: initBufferPool
*
* Description:
*      initBufferPool function is used to create a buffer pool for an existing page file
*
* Parameters:
*      BM_BufferPool *const bm
*      const char *const pageFileName
*      const int numPages
*      ReplacementStrategy strategy
*      void *stratData
*
* Return:
*      RC Name                           Value                           Comment:
*      RC_OK                               0                        Process successful
*
* Author: Devika Beniwal
*
*
*
* History:
*      Date         Name                                                    Content
*      ----------   ---------------------------------         ---------------------
*      2016-09-25   Sarthak Anand <sanand13@hawk.iit.edu>       Add comments and header comment
*
***********************************************************************************/

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,const int numPages, ReplacementStrategy strategy,void *stratData){

	queuePool = numPages;
	DLnode *newnode = malloc(sizeof(DLnode) * queuePool);// allocate memory to buffer pool
	writeNum = 0;
	int start = 0;
	int initialize = 0;
  // initialize the buffer pool to store the pages from disk
  while(start < queuePool){
	  newnode[start].handle_pg = NULL;
	  newnode[start].nodeNumPage = initialize - 1;
	  newnode[start].dirtyPage = initialize;
	  newnode[start].fixedCounter = initialize;
	  newnode[start].inMemoryCounter = initialize;
	  start++;
	}

	bm->mgmtData = newnode;
	bm->pageFile = (char *)pageFileName;
	bm->numPages = numPages;
	bm->strategy = strategy;
	return RC_OK;
}

/**********************************************************************************
* Function Name: shutdownBufferPool
*
* Description:
*     shutdownBufferPool is used to shutdown a buffer pool and free up all associated resources
*
* Parameters:
*      BM_BufferPool *const bm
*
* Return:
*      RC Name                      Value                   Comment:
*      RC_OK                               0                        Process successful
*
* Author: Damini Ganesh
*
*
*
* History:
*      Date         Name                                                    Content
*      ----------   ---------------------------------         ---------------------
*      2016-09-17
*      2016-09-22
*      2016-09-25   Sarthak Anand <sanand13@hawk.iit.edu>       Add comments and header comment
*
***********************************************************************************/


RC shutdownBufferPool(BM_BufferPool *const bm){
	int start = 0;
	forceFlushPool(bm);
	DLnode *dlNode = (DLnode *)bm->mgmtData;
	while(start < queuePool){
		if(dlNode[start].fixedCounter != 0){
			return ALREADY_IN_MEMORY;
		}
		start++;
	}
	free(dlNode);
	bm->mgmtData = NULL;
	return RC_OK;
}
/**********************************************************************************
* Function Name: forceFlushPool
*
* Description:
*      forceFlushPool is used to force the buffer manager to write all dirty pages to disk
*
* Parameters:
*      BM_BufferPool *const bm
*
* Return:
*      RC Name                      Value                   Comment:
*      RC_OK                                 0                      Process successful
*
* Author: Damini Ganesh
*
*
*
* History:
*      Date         Name                                      Content
*      ----------   ---------------------------------         ---------------------
*      2016-09-17
*      2016-09-22
*      2016-09-25   Sarthak Anand <sanand13@hawk.iit.edu>       Add comments and header comment
*
***********************************************************************************/

RC forceFlushPool(BM_BufferPool *const bm){
	DLnode *reqPage = (DLnode *)bm->mgmtData;
	int i = 0, start = 0;
		while(queuePool > i){
			int num = reqPage[i].nodeNumPage;
		if(!reqPage[i].fixedCounter && reqPage[i].dirtyPage){ // before flushing check if some page is dirty then write back to the disk
			SM_FileHandle fhandle;
			openPageFile(bm->pageFile, &fhandle);
			writeBlock(num, &fhandle, reqPage[i].handle_pg);// writing to the disk
			writeNum = writeNum+1; // increment the write counter
			reqPage[i].dirtyPage = start; // reset the dirty bit
		}
		i++;
	}
	return RC_OK;
}

/**********************************************************************************
* Function Name: pinPage
*
* Description:
*      pinPage pins the page with page number pageNum
*
* Parameters:
*      BM_BufferPool *const bm
*      BM_PageHandle *const page
*      const PageNumber pageNum
*
* Return:
*      RC Name                            Value                           Comment:
*      RC_OK                                0                       Process successful
*
* Author: Devika Beniwal
*
** History:
*      Date         Name                                                    Content
*      ----------   ---------------------------------         ---------------------
*      2016-09-25   Sarthak Anand <sanand13@hawk.iit.edu>       Add comments and header comment
*
***********************************************************************************/

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber nodeNumPage){

	DLnode *requestedPage = (DLnode *)bm->mgmtData;
	int getPageNumber = requestedPage[0].nodeNumPage;

	if(getPageNumber != -1){ //page already in buffer pool
		int start = 0;
		bool bool = true;
		while(start < queuePool){ // find the page
			int pageNum = requestedPage[start].nodeNumPage;
			if(pageNum != -1){
				if(requestedPage[start].nodeNumPage == nodeNumPage){ // already been read before increment fix counter by 1
					requestedPage[start].fixedCounter = requestedPage[start].fixedCounter+1;
					bool = false;
					matchfactor= matchfactor + 1;
					page->pageNum = nodeNumPage; // assign page number
					page->data = requestedPage[start].handle_pg;
					break;
				}
			} else { // if page number is -1
                int initial = 0;
                requestedPage[start].handle_pg = (SM_PageHandle) malloc(PAGE_SIZE);
                int pg = nodeNumPage;
				SM_FileHandle fhandle;
				openPageFile(bm->pageFile, &fhandle); // open file
				readBlock(nodeNumPage, &fhandle, requestedPage[start].handle_pg); // read the page number into buffer
				matchfactor = matchfactor+1;
				requestedPage[start].nodeNumPage = pg; // assign the page number
				readNum = readNum + 1; // increment read counter
				initial++;
				requestedPage[start].fixedCounter = initial;

				page->pageNum = nodeNumPage;
				page->data = requestedPage[start].handle_pg;
				bool = false;
				break;
			}

			start = start+1;
		}

		if(bool){ // replace page

			DLnode *reqPage = (DLnode *) malloc(sizeof(DLnode));
			reqPage->handle_pg = (SM_PageHandle) malloc(PAGE_SIZE);
            int start = 0;
            SM_FileHandle fhandle;
			openPageFile(bm->pageFile, &fhandle);// open page file
			readBlock(nodeNumPage, &fhandle, reqPage->handle_pg);// read page file
			matchfactor = matchfactor + 1;
		    readNum = readNum+1;

			reqPage->nodeNumPage = nodeNumPage;// assign to page number

			reqPage->dirtyPage = start;
			start = start+1;
			reqPage->fixedCounter = start;
			page->pageNum = nodeNumPage;
			page->data = reqPage->handle_pg;
			if(bm->strategy == RS_FIFO){
				pinPageFIFO(bm, reqPage);
			}
		}
		return RC_OK;
	}else{ // page is  does not exist in buffer pool
			SM_FileHandle fhandle;
			int first = 0;
			openPageFile(bm->pageFile, &fhandle);
			requestedPage[first].handle_pg = (SM_PageHandle) malloc(PAGE_SIZE);//Allocate memory
			ensureCapacity(nodeNumPage,&fhandle);
			readBlock(nodeNumPage, &fhandle, requestedPage[first].handle_pg); // read from disk into buffer pool
			requestedPage[first].fixedCounter++;
			requestedPage[first].nodeNumPage = nodeNumPage;
			readNum = matchfactor = first;
			requestedPage[first].inMemoryCounter = matchfactor; // increment hit counter
			page->data = requestedPage[first].handle_pg; // set into page handle
			page->pageNum = nodeNumPage; // set page number too
			return RC_OK;
	}
}
/**********************************************************************************
* Function Name: markDirty
*
* Description:
*      markDirty function is used to mark a page as dirty ie, the contents of that page have been modified
*
* Parameters:
*      BM_BufferPool *const bm
*      BM_PageHandle *const page
*
* Return:
*      RC Name                      Value                   Comment:
*      INTERNAL_ERROR                404                   Internal error
*      RC_OK                                  0                     Process successful
*
* Author:Devika Beniwal
*
*
*
* History:
*      Date         Name                                                    Content
*      ----------   ---------------------------------         ---------------------
*      2016-09-17
*      2016-09-22
*      2016-09-25                                             Add comments and header
*
***********************************************************************************/

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
	DLnode *reqPage = (DLnode *)bm->mgmtData;

	int l = 0, start = 0;

	while(queuePool > l){
		int pg =  page->pageNum;
		if(reqPage[l].nodeNumPage != pg){
			 l++;
            continue;
		}else{
			reqPage[l].dirtyPage = start;// find the page from buffer pool and mark it as dirty
			return RC_OK;
		}
	}
	return INTERNAL_ERROR;
}

/**********************************************************************************
* Function Name: unpinPage
*
* Description:
*      unpinPage function marks a page to be unpinned , ie, it is not a regularly
*    used page and can be removed from the buffer
*
* Parameters:
*      BM_BufferPool *const bm
*      BM_PageHandle *const page
*
* Return:
*      RC Name                             Value                   Comment:
*      RC_OK                                0                       Process successful
*
* Author:Devika Beniwal
*
*
***********************************************************************************/

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
	DLnode *reqPage = (DLnode *)bm->mgmtData;

	int i = 0;
    while(queuePool > i){
		if(reqPage[i].nodeNumPage != page->pageNum){
			i++;
			continue;
		}else{
			reqPage[i].fixedCounter = reqPage[i].fixedCounter - 1;// find the page from buffer pool and unpin the page by reducing its fixed counter
			break;
		}
	}
	return RC_OK;
}
/**********************************************************************************
* Function Name: forcePage
*
* Description:
*      forcePage writes the current content of the page back to the page file on disk
*
* Parameters:
*      BM_BufferPool *const bm
*      BM_PageHandle *const page
*
* Return:
*      RC Name                                       Value                   Comment:
*      RC_OK                                          0                       Process successful
*
* Author:
*      Devika Beniwal <dbeniwal1@hawk.iit.edu>
*
*
***********************************************************************************/

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
	DLnode *reqPage = (DLnode *)bm->mgmtData;

	int j , start = 0;
	for(j = 0; j < queuePool; j++){
		if(reqPage[j].nodeNumPage != page->pageNum){
			continue;
		}else{  // find the page number from the buffer needs to be write back into the disk

			int pg = reqPage[j].nodeNumPage;
			SM_FileHandle fhandle;
			openPageFile(bm->pageFile, &fhandle); // open page file
			writeBlock(pg, &fhandle, reqPage[j].handle_pg); // write page number to the disk
			writeNum = writeNum+1;
			reqPage[j].dirtyPage = start; // reset the dirty page to 0
		}
	}
	return RC_OK;
}


PageNumber *getFrameContents (BM_BufferPool *const bm){
	DLnode *requestedPage = (DLnode *) bm->mgmtData;
	PageNumber *page = malloc(sizeof(PageNumber) * queuePool);

	int i = 0;
	for(i=0;i < queuePool;i++) {
		int pagenumber = requestedPage[i].nodeNumPage;
		if(pagenumber != -1){  // get all the page number which are occupied inside the buffer pool
			page[i] = pagenumber;
		}else{
			page[i] = -1;
		}
	}
	return page;
}

/**********************************************************************************
* Function Name: getDirtyFlags
*
* Description:
*      The getDirtyFlags function returns an array of bools (of size numPages) where the
*    ith element is TRUE if the page stored in the ith page frame is dirty. Empty page
*    frames are considered as clean.
*
* Parameters:
*      BM_BufferPool *const bm
*
* Return:
*      RC Name                      Value                   Comment:
*      -                        -               -
*
* Author:
*      Sarthak Anand <sanand13@hawk.iit.edu>
*
*
* History:
*      Date         Name                                                    Content
*      ----------   ---------------------------------         ---------------------
*      2016-10-17   Sarthak Anand <sanand13@hawk.iit.edu>       Initialization.
*      2016-10-25   Sarthak Anand <sanand13@hawk.iit.edu>       Add comments and headers
*
***********************************************************************************/

bool *getDirtyFlags (BM_BufferPool *const bm){
	bool *dirtyPages = malloc(sizeof(bool) * queuePool);
	DLnode *requestedPage = (DLnode *)bm->mgmtData;

	int i = 0;
	while(queuePool > i ){
		int dirty = requestedPage[i].dirtyPage; // get all the frames whose dirty flag is 1
		if(dirty == 1){
			dirtyPages[i] = true;
		}else{
			dirtyPages[i] = false;
		}
		i++;
	}
	 // return those flags
	return dirtyPages;
}

/**********************************************************************************
* Function Name: getFixCounts
*
* Description:
*      The getFixCounts function returns an array of ints (of size numPages) where the
*    ith element is the fix count of the page stored in the ith page frame. Return 0
*    for empty page frames.
*
* Parameters:
*      BM_BufferPool *const bm
*
* Return:
*      RC Name                      Value                   Comment:
*      -                        -               -
*
* Author:
*      Sarthak Anand <sanand13@hawk.iit.edu>
*
*
* History:
*      Date         Name                                                    Content
*      ----------   ---------------------------------         ---------------------
*      2016-10-17   Sarthak Anand <sanand13@hawk.iit.edu>       Initialization.
*      2016-10-25   Sarthak Anand <sanand13@hawk.iit.edu>       Add comments and headers
*
***********************************************************************************/


int *getFixCounts (BM_BufferPool *const bm){
	DLnode *requestedPage= (DLnode *)bm->mgmtData;
	int *fixedCnt = malloc(sizeof(int) * queuePool);
	int i;
	for(i=0; i < queuePool;i++){
		int fix = requestedPage[i].fixedCounter; // get all the fix counts flags whose counter is greater than -1
		if(fix == -1){
			fixedCnt[i] = 0;
		}else{
			fixedCnt[i] = fix;
		}
	}
	return fixedCnt;
}
/**********************************************************************************
* Function Name: getNumReadIO
*
* Description:
*      The getNumReadIO function returns the number of pages that have been read from
*    disk since a buffer pool has been initialized.
*
* Parameters:
*      BM_BufferPool *const bm
*
* Return:
*      RC Name                      Value                   Comment:
*      -                        -               -
*
* Author:
*
*
*
* History:
*      Date         Name                                                    Content
*      ----------   ---------------------------------         ---------------------
*      2016-10-17                                               Initialization.
*      2016-10-25   Sarthak Anand <sanand13@hawk.iit.edu>       Add comments and headers
*
***********************************************************************************/

extern int getNumReadIO (BM_BufferPool *const bm){
	int getnum = readNum + 1; // return number of reads
	return getnum;
}

/**********************************************************************************
* Function Name: getNumWriteIO
*
* Description:
*      getNumWriteIO returns the number of pages written to the page file since the buffer
*    pool has been initialized.
*
* Parameters:
*      BM_BufferPool *const bm
*
* Return:
*      RC Name                      Value                   Comment:
*      -                        -               -
*
* Author:
*
*
*
* History:
*      Date         Name                                                    Content
*      ----------   ---------------------------------         ---------------------
*      2016-10-17                                              Initialization.
*      2016-10-25   Sarthak Anand <sanand13@hawk.iit.edu>       Add comments and headers
*
***********************************************************************************/
extern int getNumWriteIO (BM_BufferPool *const bm){
	return writeNum; // return number of writes
}



