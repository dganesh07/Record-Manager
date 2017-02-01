
/*
 * Author : Devika Beniwal
 *
 */

#include <math.h>

typedef struct DLnode{
	PageNumber nodeNumPage;
	int inMemoryCounter;
	int fixedCounter;
	int dirtyPage;
	SM_PageHandle handle_pg;
} DLnode;

int readNum = 0;
int writeNum = 0;
int matchfactor = 0;
int queuePool = 0;

int getReplacedPage(num, total){
    int pageToBeReplaced;
    pageToBeReplaced = (num % total);
    return pageToBeReplaced;
}

void pinPageFIFO(BM_BufferPool *const bm, DLnode *page)
{
	 int i = 0, replaced;
	 DLnode *requestedPage = (DLnode *) bm->mgmtData;
	 replaced = getReplacedPage(readNum, queuePool);// get the page to be replaced

	while(i < queuePool){
		int fixed = requestedPage[replaced].fixedCounter; // replace page whose fixed counter is greater than 0
		if(fixed != 0){
			replaced = replaced + 1;
			int temp = getReplacedPage(replaced, queuePool);
			if(temp == 0){
				replaced = 0;
			}
		}else{
			int dirty = requestedPage[replaced].dirtyPage;
			bool check = false;
			if(dirty != 1){
				check = true;
             }else{
            	 int reppagenum = requestedPage[replaced].nodeNumPage;
            	 SM_FileHandle fhandle;
				 openPageFile(bm->pageFile, &fhandle);
				 writeBlock(reppagenum, &fhandle, requestedPage[replaced].handle_pg);
				 check = true;
				 writeNum = writeNum+1;
             }

			if(check){
                int pg = page->nodeNumPage;
				int dirpg = page->dirtyPage;
				int fixcnt = page->fixedCounter;

				requestedPage[replaced].nodeNumPage = pg;
				requestedPage[replaced].fixedCounter = fixcnt;
				requestedPage[replaced].dirtyPage = dirpg;
				requestedPage[replaced].handle_pg = page->handle_pg;

			}

			break;
		}
		i = i + 1;
	}
}
