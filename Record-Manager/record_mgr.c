
/*
 * Author : Devika Beniwal
 * Desc : Record Manager
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "math.h"

const int TOTAL_PAGES_BUFFER = 100;
const int COLUMN_NAME_SIZE = 20;

typedef struct tableAndRecordInfo {

	BM_PageHandle handle_pg;
	BM_BufferPool pool_buffer;
	int row_counter;
	int free_page_counter;

} tableAndRecordInfo;
tableAndRecordInfo *rm_mgr;

typedef struct scanInfo{
	RID scanRecordID;
    Expr *scanCondition;
    int numItemsScanned;
    BM_PageHandle handle_pg;
}scanInfo;


extern Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes,int *typeLength, int keySize, int *keys) {

	Schema *newSchema = (Schema *) malloc(sizeof(Schema));
	newSchema->numAttr = numAttr;
	newSchema->attrNames = attrNames;
	newSchema->dataTypes = dataTypes;
	newSchema->typeLength = typeLength;
	newSchema->keySize = keySize;
	newSchema->keyAttrs = keys;
	return newSchema;
}

RC initRecordManager(void *mgmtData) {
	initStorageManager();
	return RC_OK;
}

RC createTable(char *name, Schema *schema) {

	SM_FileHandle fHandle;

	if (createPageFile(name) != RC_OK) { // create a page file to store table
		return RC_FILE_NOT_FOUND;
	}
	if (openPageFile(name, &fHandle) != RC_OK) { //open the page file created
		return RC_FILE_NOT_FOUND;
	}

	char pagefile[PAGE_SIZE];  // create a page file to store tuple information
	char *pagefileptr;
	pagefileptr = &pagefile[0];   // storing the address of the page-file created above

	rm_mgr = (tableAndRecordInfo *) malloc(sizeof(tableAndRecordInfo)); //get an object of tableAndRecordInfo
	initBufferPool(&rm_mgr->pool_buffer, name, TOTAL_PAGES_BUFFER, RS_FIFO, NULL); // initialize buffer pool with FIFO strategy

	*(int*)pagefileptr = 0;  // position 0 to 3 contains 0 value
	*(int*)(pagefileptr = pagefileptr + sizeof(int)) = 1; // position 4-7 contains value 1
	*(int*)(pagefileptr = pagefileptr + sizeof(int)) = schema->numAttr; // Position 8-11 contains value 3
	*(int*)(pagefileptr = pagefileptr + sizeof(int)) = schema->keySize;// Position 12-15 contains value 1 (key-size)
	pagefileptr += sizeof(int);

	int m = 0;
	while(m < schema->numAttr){
		strncpy(pagefileptr, schema->attrNames[m], COLUMN_NAME_SIZE); //Set first attribute name
		*(int*)(pagefileptr = pagefileptr + COLUMN_NAME_SIZE) = (int)schema->dataTypes[m]; //set its data-type
		*(int*)(pagefileptr = pagefileptr + sizeof(int)) = (int) schema->typeLength[m]; // set its length
		pagefileptr = pagefileptr + sizeof(int);
        m++;
	}

	if (writeBlock(0, &fHandle, pagefile) != RC_OK) {  // write created page-file(containing table information) to disk
		return RC_WRITE_FAILED;
	}
	if (closePageFile(&fHandle) != RC_OK) {
		return RC_FILE_NOT_FOUND;
	}
	return RC_OK;
}

RC openTable (RM_TableData *rel, char *name){

	SM_PageHandle pagefileptr;
	 int num_of_attributes;
	 int startPage = 0;
	Schema *newShm = (Schema*) malloc(sizeof(Schema));

    pinPage(&rm_mgr->pool_buffer, &rm_mgr->handle_pg, startPage); // get 0th page into buffer pool from the disk
    pagefileptr = (char*)rm_mgr->handle_pg.data;
    int rows = rm_mgr->handle_pg.data[0];
    rm_mgr->row_counter =  (int)rows; // assign the number of tuples in  a table
    rm_mgr->free_page_counter = *(int*)(pagefileptr = pagefileptr+sizeof(int));  // store free-pages from page file

    num_of_attributes = *(int*)(pagefileptr = pagefileptr+sizeof(int)); // No of attributes containing
    pagefileptr += sizeof(int);

    newShm->numAttr = num_of_attributes;  // store table information into schema object
    int sizeChar = sizeof(char*) * num_of_attributes;
    int datatype = sizeof(DataType) * num_of_attributes;
    int length = sizeof(int) * num_of_attributes;
    newShm->attrNames= (char**) malloc(sizeChar);
    newShm->dataTypes= (DataType*) malloc(datatype);
    newShm->typeLength= (int*) malloc(length);

    int start = 0, num_char_attr = COLUMN_NAME_SIZE;
    while(start < num_of_attributes){
    	newShm->attrNames[start] = (char*)malloc(num_char_attr); // no of characters each attributes can store = COLUMN_NAME_SIZE;
    	strncpy(newShm->attrNames[start], pagefileptr, num_char_attr); // Copy attribute name from page file to schema
    	newShm->dataTypes[start]= *(int*)(pagefileptr = pagefileptr+num_char_attr);// Copy attribute data-type from page file to schema
    	newShm->typeLength[start]= *(int*)(pagefileptr = pagefileptr+sizeof(int));// Copy attribute length from page file to schema
    	pagefileptr = pagefileptr + sizeof(int);
    	start++;
    }
    unpinPage(&rm_mgr->pool_buffer, &rm_mgr->handle_pg);

    //assign into RM_tabledata
     rel->name = name;
     rel->mgmtData = rm_mgr;
     rel->schema = newShm;

    forcePage(&rm_mgr->pool_buffer, &rm_mgr->handle_pg);
	return RC_OK;
}

int getRecordSize (Schema *schema){

	int num_columns = schema->numAttr , start = 0, eachColumnSize = 0, total = 0;
	while(start < num_columns){
     if(schema->dataTypes[start] == DT_STRING){
    	 eachColumnSize = eachColumnSize + schema->typeLength[start];
     }else if(schema->dataTypes[start] == DT_BOOL){
    	 eachColumnSize = eachColumnSize + sizeof(bool);
     }else if (schema->dataTypes[start] == DT_FLOAT){
    	 eachColumnSize = eachColumnSize +  sizeof(float);
     }else if(schema->dataTypes[start] == DT_INT){
    	 eachColumnSize = eachColumnSize +  sizeof(int);
     }
     start++;
	}
	total = eachColumnSize;
	total = total + 1;
	return total;
}


RC createRecord (Record **record, Schema *schema){
	 char *ptr;
	 int x = 0;
	 int defaultsz = -1;
	 Record *new_record = (Record*) malloc( sizeof(Record) ); // allocate memory to newly created record
	 new_record->id.page = defaultsz;
	 new_record->id.slot = defaultsz;

     int total_row_size = getRecordSize(schema); // get size of the record
     new_record->data = (char*) malloc(total_row_size); // Allocate memory for data of the tuple/record

     ptr = new_record->data;
     *(ptr + x) = '!';  // used for record when it is empty - It is acting as Tombstone
     *(ptr = ptr + sizeof(char)) = '\0'; // next slot is occupied by null char;
     *record = new_record; // point the record to newly created record
	 return RC_OK;
}

int defineDataTypeSizes(char dataType){

	int size;
	switch(dataType){
	       case DT_INT:
	    	    size = sizeof(int);
	        	break;
	        case DT_FLOAT:
	        	size = sizeof(float);
	        	break;
	        case DT_BOOL:
	        	size = sizeof(bool);
	        	break;
	}
	return size;
}


// This function sets the attribute value in the record in the specified schema

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
	int offset_from_beg = 1;
	int start = 0;
	// Getting the offset from beginning to the attribute as specified
	while(attrNum > start){
		if(schema->dataTypes[start] == DT_STRING){
			offset_from_beg = offset_from_beg + schema->typeLength[start];
		}else{
			offset_from_beg = offset_from_beg + defineDataTypeSizes(schema->dataTypes[start]);
		}
		start = start + 1;
	}
	//point the pointer to the address by adding the offset to the address
	char *ptr = record->data;
    ptr = ptr + offset_from_beg;
    // Get data from Value and set into the record at that location and increment pointer

     if(schema->dataTypes[attrNum] == DT_INT){
          *(int*)ptr  = value->v.intV;
           ptr = ptr + sizeof(int);
     }else if(schema->dataTypes[attrNum] == DT_BOOL){
    	 *(bool*)ptr  = value->v.boolV;
    	 ptr = ptr + sizeof(bool);
     }else if(schema->dataTypes[attrNum] == DT_FLOAT){
    	 *(float*)ptr  = value->v.floatV;
    	  ptr = ptr + sizeof(float);
     }else if(schema->dataTypes[attrNum] == DT_STRING){
            char *temp = malloc(sizeof(schema->typeLength[attrNum] + 1));
            strncpy(temp, value->v.stringV, schema->typeLength[attrNum]);
            temp[schema->typeLength[attrNum]] = '\0';
            strncpy(ptr, temp, schema->typeLength[attrNum]);
            ptr = ptr + schema->typeLength[attrNum];
     }else{
    	 printf("DATA TYPE WRONG");
     }
	 return RC_OK;
}
// This function gets the attribute value from the record

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){

	int offset_from_beg = 1 , start = 0;
	Value *val = (Value*)malloc(sizeof(Value));

		// Getting the offset from beginning to the attribute as specified
		while(attrNum > start){
			if(schema->dataTypes[start] == DT_STRING){
				offset_from_beg = offset_from_beg + schema->typeLength[start];
			}else{
				offset_from_beg = offset_from_beg + defineDataTypeSizes(schema->dataTypes[start]);
			}
			start = start + 1;
		}

		// now get the value from the record pointed to by ptr based on their data-type length
		char *ptr = record->data;
		ptr = ptr + offset_from_beg;

		if(attrNum != 1){
			char dt = schema->dataTypes[attrNum];
	        schema->dataTypes[attrNum] = dt;
		}else{
			schema->dataTypes[attrNum] = 1;
		}

		if(schema->dataTypes[attrNum] == DT_INT){
			  int data = 0;
			  memcpy(&data,ptr, sizeof(int));
			  val->v.intV = data;
			  val->dt = DT_INT;
		}else if(schema->dataTypes[attrNum] == DT_FLOAT){
			  float data;
			  memcpy(&data,ptr, sizeof(float));
			  val->v.floatV = data;
			  val->dt = DT_FLOAT;
		}else if(schema->dataTypes[attrNum] == DT_BOOL){
			  bool data;
			  memcpy(&data,ptr, sizeof(bool));
			  val->v.floatV = data;
			  val->dt = DT_FLOAT;
		}else if(schema->dataTypes[attrNum] == DT_STRING){
              val->v.stringV = (char*)malloc(schema->typeLength[attrNum] + 1);
              strncpy(val->v.stringV, ptr, schema->typeLength[attrNum]);
              val->v.stringV[schema->typeLength[attrNum]] = '\0';
              val->dt = DT_STRING;
		}else{
			printf("NO SUCH DATATYPE");
		}

      *value = val; // assign get value
	  return RC_OK;
}

RC freeRecord (Record *record){
     free(record);
     return RC_OK;
}

RC freeSchema (Schema *schema){
	 free(schema);
	 return RC_OK;
}

int getNumTuples (RM_TableData *rel){
	tableAndRecordInfo *rm_mgr = rel->mgmtData;
	return rm_mgr->row_counter;
}

int getSlotNumber(int totalNumSlots, char *recordData, int size){

	int num = 0;
	int returnNum = -1;

	while(num < totalNumSlots){
		 if(recordData[num * size] != '@'){
			 returnNum = num;
			 return returnNum;
		 }
		 num++;
	}
	return returnNum;
}


RC insertRecord (RM_TableData *rel, Record *record){

	RID *rid = &record->id;
	char *rcdata, *slotInfoPtr;
	int pinpg = 0;
	Schema *sch = rel->schema;
	char *temp = record->data;

	tableAndRecordInfo *rm_mgr = rel->mgmtData;
	int recordpagenumber = rm_mgr->free_page_counter;

	rid->page = recordpagenumber; // free page number
	pinPage(&rm_mgr->pool_buffer, &rm_mgr->handle_pg, recordpagenumber); //get the page into buffer
	rcdata = rm_mgr->handle_pg.data;

	int size = getRecordSize(sch); // size of each tuple
    // get the slot number which is free to insert the data
	int totalNumSlots = floor(PAGE_SIZE / size);
    rid->slot = getSlotNumber(totalNumSlots, rcdata, size); // get the slot number which is free

    while(rid->slot == -1){
    	 unpinPage(&rm_mgr->pool_buffer, &rm_mgr->handle_pg);	// unpin the page if respective page is full and increment the page number
         rid->page =  rid->page + 1;
         pinPage(&rm_mgr->pool_buffer, &rm_mgr->handle_pg, rid->page);
         rcdata = rm_mgr->handle_pg.data;
         int slotnum;
         slotnum = getSlotNumber(totalNumSlots, rcdata, size);
    	 rid->slot = slotnum;
    }

     slotInfoPtr = rcdata; //point the slot pointer to the record data
     markDirty(&rm_mgr->pool_buffer,&rm_mgr->handle_pg); // mark dirty so that we can write into disk before replacing

     slotInfoPtr = slotInfoPtr + (rid->slot * size);
     *(slotInfoPtr+pinpg) = '@'; // Tombstone
     slotInfoPtr = slotInfoPtr + 1;
     size = size - 1;
     temp = temp + 1;
     memcpy(slotInfoPtr, temp, size);

     unpinPage(&rm_mgr->pool_buffer, &rm_mgr->handle_pg);
     pinPage(&rm_mgr->pool_buffer, &rm_mgr->handle_pg, pinpg);
     rm_mgr->row_counter = rm_mgr->row_counter + 1;
     //print data and check
      /*
       char *t = rcdata;
     	for(int i = 0;i<=100;i++){
     	 printf("%d  =>   data : %d\n",i,t[i]);
     	}
      printf("*************************==============*********************************\n");
     */
     return RC_OK;
}

extern RC closeTable (RM_TableData *rel){

	tableAndRecordInfo *rm_mgr = rel->mgmtData;
    shutdownBufferPool(&rm_mgr->pool_buffer);
	return RC_OK;
}

extern RC getRecord (RM_TableData *rel, RID id, Record *record){

        int pageNumber = id.page;
        int slotNumber = id.slot;

        tableAndRecordInfo *rm_mgr = rel->mgmtData;
        pinPage(&rm_mgr->pool_buffer, &rm_mgr->handle_pg, pageNumber); //get the page number into the buffer to read the data

        int size = getRecordSize(rel->schema); // get record size till what point we need to read

        char *readDataPtr = rm_mgr->handle_pg.data;

        // Increment the pointer to the location from where we need to start reading
        int readOffset = (size * slotNumber);
        readDataPtr = readDataPtr + readOffset;
        size = size - 1;
        if(*readDataPtr == '@'){
        	record->id = id;

			//char *m = record->data;
			 // for(int i = 0;i<30;i++){
			  // 	 printf("%d  =>   data : %d\n",i,m[i]);
									//      }

        	 memcpy(record->data + 1, readDataPtr + 1, size);
        }else{
        	 return RC_RM_NO_MORE_TUPLES;
        }
         unpinPage(&rm_mgr->pool_buffer, &rm_mgr->handle_pg);
	     return RC_OK;
}


extern RC updateRecord (RM_TableData *rel, Record *record){

     RID updateID = record->id;
     int slotNumber = updateID.slot; // get slot number
     int pageNumber = updateID.page;// get page number

     char *temp = record->data;
     temp = temp + 1;

     tableAndRecordInfo *rm_mgr = rel->mgmtData;
     pinPage(&rm_mgr->pool_buffer, &rm_mgr->handle_pg, pageNumber); //get the page number into the buffer to read the data
     int size = getRecordSize(rel->schema);
     char *updateDataPtr = rm_mgr->handle_pg.data;
     updateDataPtr += size*slotNumber; // go to the particular slot number and update the data
     *updateDataPtr = '@'; // Tomb-stone indicates record is not empty
     updateDataPtr = updateDataPtr+1;
     size = size - 1;
     memcpy(updateDataPtr, temp, size); // update record
     markDirty(&rm_mgr->pool_buffer, &rm_mgr->handle_pg);
     unpinPage(&rm_mgr->pool_buffer, &rm_mgr->handle_pg);
	 return RC_OK;
}

extern RC deleteTable (char *name){
	destroyPageFile(name);
	return RC_OK;
}

extern RC shutdownRecordManager(){
    free(rm_mgr);
	return RC_OK;
}

extern RC deleteRecord (RM_TableData *rel, RID id){
     int offset  = 0;
	 tableAndRecordInfo *rm_mgr = rel->mgmtData;
	 pinPage(&rm_mgr->pool_buffer, &rm_mgr->handle_pg, id.page); //get the page number into the buffer to read the data
	 rm_mgr->free_page_counter = id.page; // set free page number
	 int size = getRecordSize(rel->schema);
	 char *deleteDataPtr = rm_mgr->handle_pg.data;
	 deleteDataPtr += size*id.slot;
	 *(deleteDataPtr + offset) = '!'; //Tombstone : marking it as free page which can be used later on  as we have deleted the record
	 deleteDataPtr++;
	 markDirty(&rm_mgr->pool_buffer, &rm_mgr->handle_pg);
	 unpinPage(&rm_mgr->pool_buffer, &rm_mgr->handle_pg);
	 return RC_OK;
}


extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){

	if(cond != NULL){
		int start = 0;
		openTable(rel, "NewScanningTable");
		scanInfo *scanPtr = (scanInfo *)malloc(sizeof(scanInfo));
		scan->mgmtData = scanPtr;
		scanPtr->scanCondition = cond; // assign condition as given
		scanPtr->scanRecordID.slot =  scanPtr->numItemsScanned = start; // start scanning with slot number 0 and number of items scanned are  0 yet
		start++;
		scanPtr->scanRecordID.page = start; // start scanning with page number 1

		tableAndRecordInfo *tableRm_mgr;
		tableRm_mgr = rel->mgmtData;
		tableRm_mgr->row_counter = COLUMN_NAME_SIZE;
		scan->rel= rel;
		return RC_OK;
	}else{
		return RC_NO_QUERY_CONDITION_EXIST;
	}
}

extern RC next (RM_ScanHandle *scan, Record *record){

	Schema *newSch = scan->rel->schema;
	scanInfo *scanPtr = scan->mgmtData; // assign the scanning data

	tableAndRecordInfo *tableRm_mgr = (tableAndRecordInfo*)scan->rel->mgmtData;
	Value *val = (Value *) malloc(sizeof(Value)); // allocate memory to Value
    int start = 0;

	if(scanPtr->scanCondition != NULL){
		int size = getRecordSize(newSch); // get the size of each tuple
		int totalNUmSlots = floor(PAGE_SIZE / size);
		int no_scanned_items = scanPtr->numItemsScanned;
		int total_num_rows = tableRm_mgr->row_counter;  // check this out

	//	printf("\n scannedItms : %d  Rows : %d\n",no_scanned_items, total_num_rows);

         while(total_num_rows >= no_scanned_items){
               if(start >= no_scanned_items){ // if nothing has scanned yet
            	   scanPtr->scanRecordID.slot = 0; // initialize scan to 0th position
            	   scanPtr->scanRecordID.page = 1; // initialize page to 1st position
               }else{
            	   scanPtr->scanRecordID.slot = scanPtr->scanRecordID.slot + 1; // increment both slot number
                   // reset slot number if all the slots have been scanned
            	   if(totalNUmSlots <= scanPtr->scanRecordID.slot){
            		   scanPtr->scanRecordID.page = scanPtr->scanRecordID.page + 1;
            		   scanPtr->scanRecordID.slot = 0;
            	   }
               }
               int pageNum = scanPtr->scanRecordID.page;
               // scanning start by pinning the page into the buffer
               pinPage(&tableRm_mgr->pool_buffer, &scanPtr->handle_pg, pageNum);
               char *pagefileptr = scanPtr->handle_pg.data;
               int location = (scanPtr->scanRecordID.slot * size);
               int tempSize = size - 1;
               pagefileptr = pagefileptr + location; // set the pointer from where it needs to start scanning
               int getSlotNum = scanPtr->scanRecordID.slot;
               int getPageNum = scanPtr->scanRecordID.page;
               // set into record data the slot and page number
               record->id.page  = getPageNum;
               record->id.slot = getSlotNum;
             // 	printf("\n SLOT : %d  PAGE : %d",getSlotNum, getPageNum);

               //copy data from page-file from buffer (disk) into the record data to scan the table
                char *recordPtr = record->data;
                *(recordPtr+start) = '!';
                recordPtr = recordPtr + 1;

                memcpy(recordPtr, pagefileptr + 1 , tempSize);
                //increase the scan count
                no_scanned_items = no_scanned_items + 1;
                scanPtr->numItemsScanned = no_scanned_items;

                // Now evaluate the expression for required tuples
                evalExpr(record, newSch, scanPtr->scanCondition, &val);

                if(val->v.boolV == FALSE){
                  continue;
                }else{
                	unpinPage(&tableRm_mgr->pool_buffer, &scanPtr->handle_pg);
                	return RC_OK;
                }
         }

         unpinPage(&tableRm_mgr->pool_buffer, &scanPtr->handle_pg); //unpin the page

         //Reset record data after insertion
         scanPtr->scanRecordID.slot = start;
         scanPtr->numItemsScanned = start;
         start++;
         scanPtr->scanRecordID.page = start;
         return RC_RM_NO_MORE_TUPLES;
	}else{
		return RC_NO_QUERY_CONDITION_EXIST;
	}
}

extern RC closeScan (RM_ScanHandle *scan){

	scan->mgmtData = NULL;
	free(scan->mgmtData);
    return RC_OK ;
}





