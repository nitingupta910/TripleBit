//---------------------------------------------------------------------------
// TripleBit
// (c) 2011 Massive Data Management Group @ SCTS & CGCL. 
//     Web site: http://grid.hust.edu.cn/triplebit
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------

#include "SortMergeJoin.h"
#include "../EntityIDBuffer.h"

SortMergeJoin::SortMergeJoin() {
	// TODO Auto-generated constructor stub
	//pool = new CThreadPool(THREAD_NUMBER);
	temp1 = (ID*)malloc(4096 * sizeof(ID));
	temp2 = (ID*)malloc(4096 * sizeof(ID));
}

SortMergeJoin::~SortMergeJoin() {
	// TODO Auto-generated destructor stub
	if(temp1 != NULL)
		free(temp1);
	temp1 = NULL;

	if(temp2 != NULL)
		free(temp2);
	temp2 = NULL;
}

/*
 * @param
 * secondModify: 是否要修改entBuffer2;
 */
void SortMergeJoin::Join(EntityIDBuffer* entBuffer1, EntityIDBuffer* entBuffer2, int joinKey1, int joinKey2, bool secondModify /* = true */)
{
	entBuffer1->sort(joinKey1);
	entBuffer2->sort(joinKey2);

	joinKey1--;
	joinKey2--;

//	cout<<"============="<<endl;
//	entBuffer1->print();
//	cout<<"-------------"<<endl;
//	entBuffer2->print();

//	int size1 = entBuffer1->getSize();
//	int size2 = entBuffer2->getSize();

	/*char* flag1 = (char*)malloc(size1);
	memset(flag1,0,size1);
	char* flag2;
	if( secondModify == true) {
		flag2 = (char*)malloc(size2);
		memset(flag2,0,size2);
	} else {
		flag2 = NULL;
	}*/

//	cout<<"iSize: "<<size1<<" jSize: "<<size2<<endl;

	//ID* buffer1 = entBuffer1->getBuffer();
	//ID* buffer2 = entBuffer2->getBuffer();

	//SortMergeJoinArg* arg;
	//arg = new SortMergeJoinArg(buffer1, buffer2, size1, size2, flag1, flag2, entBuffer1->IDCount, entBuffer2->IDCount, joinKey1, joinKey2);
	//Merge(arg);
	if(secondModify) {
		Merge1(entBuffer1, entBuffer2, joinKey1, joinKey2);
	} else {
		Merge2(entBuffer1, entBuffer2, joinKey1, joinKey2);
	}

	/*entBuffer1->usedSize = arg->length1;
	entBuffer2->usedSize*/
	/*
	SortMergeJoinArg* arg;

	int length1 = size1 / THREAD_NUMBER;
	int length2 = size2 / THREAD_NUMBER;

	int startPos1 = 0;
	int startPos2 = 0;

	for( int i = 0; i < THREAD_NUMBER; i++ ) {
		if ( i < THREAD_NUMBER - 1 ) {
			if ( secondModify == true )
				arg = new SortMergeJoinArg(buffer1 + startPos1 * entBuffer1->IDCount, buffer2 + startPos2 * entBuffer2->IDCount, length1, length2,
						flag1 + startPos1, flag2 + startPos2, entBuffer1->IDCount, entBuffer2->IDCount, joinKey1, joinKey2);
			else
				arg = new SortMergeJoinArg(buffer1 + startPos1 * entBuffer1->IDCount, buffer2 + startPos2 * entBuffer2->IDCount, length1, length2,
						flag1 + startPos1, NULL, entBuffer1->IDCount, entBuffer2->IDCount, joinKey1, joinKey2);
			CThreadPool::getInstance().AddTask(boost::bind(&SortMergeJoin::Merge, this, arg));
			startPos1 += length1;
			startPos2 += length2;
		} else {
			if ( secondModify == true )
				arg = new SortMergeJoinArg(buffer1 + startPos1 * entBuffer1->IDCount, buffer2 + startPos2 * entBuffer2->IDCount,
						size1 - startPos1, size2 - startPos2, flag1 + startPos1, flag2 + startPos2, entBuffer1->IDCount, entBuffer2->IDCount, joinKey1, joinKey2);
			else
				arg = new SortMergeJoinArg(buffer1 + startPos1 * entBuffer1->IDCount, buffer2 + startPos2 * entBuffer2->IDCount,
						size1 - startPos1, size2 - startPos2, flag1 + startPos1, NULL, entBuffer1->IDCount, entBuffer2->IDCount, joinKey1, joinKey2);
			CThreadPool::getInstance().AddTask(boost::bind(&SortMergeJoin::Merge, this, arg));
		}
	}

	CThreadPool::getInstance().Wait();
	*/

	/*entBuffer1->modifyByFlag(flag1,1);
	if ( secondModify )
	{
		entBuffer2->modifyByFlag(flag2,1);
	}

	free(flag1);
	if ( secondModify )
		free(flag2);*/

}

int SortMergeJoin::Merge(SortMergeJoinArg* arg)
{
	/*if ( arg->flag2 != NULL )
		Merge1(arg);
	else
		Merge2(arg);*/

	//delete arg;
	return 0;
}

void SortMergeJoin::Merge1(EntityIDBuffer* entBuffer1, EntityIDBuffer* entBuffer2, int joinKey1, int joinKey2)
{
	register size_t i = 0;
	register size_t j = 0;
	register int k;
	register size_t pos1 = 0, pos2 = 0;
	size_t size1 = 0, size2 = 0;
	register ID keyValue;

	ID* buffer1 = entBuffer1->getBuffer();
	ID* buffer2 = entBuffer2->getBuffer();
	size_t length1 = entBuffer1->getSize();
	size_t length2 = entBuffer2->getSize();
	int IDCount1 = entBuffer1->getIDCount();
	int IDCount2 = entBuffer2->getIDCount();

	while ( i < length1 && j < length2 ) {
		keyValue = buffer1[i * IDCount1 + joinKey1];

		while (buffer2[j * IDCount2 + joinKey2] < keyValue && j < length2) {
			j++;
		}

		if (buffer2[j * IDCount2 + joinKey2] == keyValue) {
			while (buffer1[i * IDCount1 + joinKey1] == keyValue && i < length1) {
				if(pos1 == 4096) {
					memcpy(buffer1 + size1, temp1, 4096 * sizeof(ID));
					size1 = size1 + pos1;
					pos1 = 0;
				}

				for (k = 0; k < IDCount1; k++) {
					temp1[pos1] = buffer1[i * IDCount1 + k];
					pos1++;
				}
				
//				memcpy(temp1 + pos1, buffer1 + i * IDCount1, IDCount1 * sizeof(ID));
//				pos1 += IDCount1;
				i++;
			}

			while (buffer2[j * IDCount2 + joinKey2] == keyValue && j < length2) {
				if(pos2 == 4096) {
					memcpy(buffer2 + size2, temp2, 4096 * sizeof(ID));
					size2 = size2 + pos2;
					pos2 = 0;
				}

				for (k = 0; k < IDCount2; k++) {
					temp2[pos2] = buffer2[j * IDCount2 + k];
					pos2++;
				}

				//memcpy(temp2 + pos2, buffer2 + j * IDCount2, IDCount2 * sizeof(ID));
//				pos2 += IDCount2;
				j++;
			}
		} else {
			while (buffer1[i * IDCount1 + joinKey1] == keyValue && i < length1) {
				i++;
			}
		}
	}

	memcpy(buffer1 + size1, temp1, pos1 * sizeof(ID));
	size1 = size1 + pos1;
	entBuffer1->usedSize = size1;

	memcpy(buffer2 + size2, temp2, pos2 * sizeof(ID));
	size2 = size2 + pos2;
	entBuffer2->usedSize = size2;
}

void SortMergeJoin::Merge2(EntityIDBuffer* entBuffer1, EntityIDBuffer* entBuffer2, int joinKey1, int joinKey2)
{	
	register int i = 0;
	register int j = 0;
	register int k;
	register size_t pos1 = 0;
	size_t size1 = 0;
	register ID keyValue;

	ID* buffer1 = entBuffer1->getBuffer();
	ID* buffer2 = entBuffer2->getBuffer();
	int length1 = entBuffer1->getSize();
	int length2 = entBuffer2->getSize();
	int IDCount1 = entBuffer1->getIDCount();
	int IDCount2 = entBuffer2->getIDCount();

	while ( i < length1 && j < length2 ) {
		keyValue = buffer1[i * IDCount1 + joinKey1];

		while (buffer2[j * IDCount2 + joinKey2] < keyValue && j < length2) {
			j++;
		}

		if (buffer2[j * IDCount2 + joinKey2] == keyValue) {
			while (buffer1[i * IDCount1 + joinKey1] == keyValue && i < length1) {
				if(pos1 == 4096) {
					memcpy(buffer1 + size1, temp1, 4096 * sizeof(ID));
					size1 = size1 + pos1;
					pos1 = 0;
				}

				for (k = 0; k < IDCount1; k++) {
					temp1[pos1] = buffer1[i * IDCount1 + k];
					pos1++;
				}
				i++;
			}

			while (buffer2[j * IDCount2 + joinKey2] == keyValue && j < length2) {
				j++;
			}
		} else {
			while (buffer1[i * IDCount1 + joinKey1] == keyValue && i < length1) {
				i++;
			}
		}
	}

	memcpy(buffer1 + size1, temp1, pos1 * sizeof(ID));
	size1 = size1 + pos1;
	entBuffer1->usedSize = size1;
}
