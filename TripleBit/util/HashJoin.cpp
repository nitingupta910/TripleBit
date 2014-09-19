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

#include "HashJoin.h"
#include "../EntityIDBuffer.h"

#include <math.h>

HashJoin::HashJoin() {
	// TODO Auto-generated constructor stub
	//pool = new CThreadPool(THREAD_NUMBER);
}

HashJoin::~HashJoin() {
	// TODO Auto-generated destructor stub
}

void HashJoin::SortMergeJoin(ID* buffer1, ID* buffer2, int IDCount1, int IDCount2,
		int joinKey1, int joinKey2, int size1, int size2, char* flagVector1, char* flagVector2)
{
	int i, j;
	ID keyValue;

	//sort the buffer;
	if ( IDCount1 == 1) {
		std::sort(&buffer1[0], &buffer1[size1], SortTask::compareInt);
	} else if ( joinKey1 == 0) {
		/*
		std::sort((int64_t*)&buffer1[0], (int64_t*)&buffer1[size1 * 2], SortTask::compareLongByFirst32);
		*/
		qsort((int64_t*)&buffer1[0], size1, sizeof(int64_t), SortTask::qcompareLongByFirst32);
	} else if ( joinKey1 == 1) {
		/*
		std::sort((int64_t*)&buffer1[0], (int64_t*)&buffer1[size1 * 2], SortTask::compareLongBySecond32);
		*/
		qsort((int64_t*)&buffer1[0], size1, sizeof(int64_t), SortTask::qcompareLongBySecond32);
	}

	if ( IDCount2 == 1 ) {
		std::sort(&buffer2[0], &buffer2[size2], SortTask::compareInt);
	} else if ( joinKey2 == 0 ){
		/*
		std::sort((int64_t*)&buffer2[0], (int64_t*)&buffer2[size2 * 2], SortTask::compareLongByFirst32);
		*/
		 qsort((int64_t*)&buffer2[0], size2, sizeof(int64_t), SortTask::qcompareLongByFirst32);
	} else if ( joinKey2 == 1 ) {
		/*
		std::sort((int64_t*)&buffer2[0], (int64_t*)&buffer2[size2 * 2], SortTask::compareLongBySecond32);
		*/
		 qsort((int64_t*)&buffer2[0], size2, sizeof(int64_t), SortTask::qcompareLongBySecond32);
	}

	i = 0;
	j = 0;

	while ( i < size1 && j < size2 ) {
		keyValue = buffer1[i * IDCount1 + joinKey1];

		while (buffer2[j * IDCount2 + joinKey2] < keyValue && j < size2) {
			j++;
		}

		if (buffer2[j * IDCount2 + joinKey2] == keyValue) {
			while (buffer1[i * IDCount1 + joinKey1] == keyValue && i < size1) {
				flagVector1[i]++;
				i++;

			}

			while (buffer2[j * IDCount2 + joinKey2] == keyValue && j < size2) {
				flagVector2[j]++;
				j++;

			}
		} else {
			while (buffer1[i * IDCount1 + joinKey1] == keyValue && i < size1) {
				i++;
			}
		}
	}
}

void HashJoin::SortMergeJoin(ID* buffer1, ID* buffer2, int IDCount1, int IDCount2,
		int joinKey1, int joinKey2, int size1, int size2, char* flagVector1)
{
	int i, j;
	ID keyValue;

	//cout<<"size1: "<<size1<<" size2: "<<size2<<endl;
	//sort the buffer;
	if ( IDCount1 == 1) {
		std::sort(buffer1, buffer1 + size1, SortTask::compareInt);
	} else if ( joinKey1 == 0) {
		/*
		std::sort((int64_t*)buffer1, (int64_t*)(buffer1 + size1 * IDCount1), SortTask::compareLongByFirst32);
		*/
		 qsort((int64_t*)&buffer1[0], size1, sizeof(int64_t), SortTask::qcompareLongByFirst32);
	} else if ( joinKey1 == 1) {
		/*
		std::sort((int64_t*)buffer1, (int64_t*)(buffer1 + size1 * IDCount1), SortTask::compareLongBySecond32);
		*/
		 qsort((int64_t*)&buffer1[0], size1, sizeof(int64_t), SortTask::qcompareLongBySecond32);
	}

	if ( IDCount2 == 1 ) {
		std::sort(buffer2, buffer2 + size2, SortTask::compareInt);
	} else if ( joinKey2 == 0 ){
		/*
		std::sort((int64_t*)buffer2, (int64_t*)(buffer2 + size2 * IDCount2), SortTask::compareLongByFirst32);
		*/
		 qsort((int64_t*)&buffer2[0], size2, sizeof(int64_t), SortTask::qcompareLongByFirst32);
	} else if ( joinKey2 == 1 ) {
		/*
		std::sort((int64_t*)buffer2, (int64_t*)(buffer2 + size2 * IDCount2), SortTask::compareLongBySecond32);
		*/
		 qsort((int64_t*)&buffer2[0], size2, sizeof(int64_t), SortTask::qcompareLongBySecond32);
	}

	i = j = 0;
	while ( i < size1 && j < size2 ) {
		keyValue = buffer1[i * IDCount1 + joinKey1];

		while (buffer2[j * IDCount2 + joinKey2] < keyValue && j < size2) {
			j++;
		}

		if (buffer2[j * IDCount2 + joinKey2] == keyValue) {
			while (buffer1[i * IDCount1 + joinKey1] == keyValue && i < size1) {
				flagVector1[i]++;
				i++;

			}

			while (buffer2[j * IDCount2 + joinKey2] == keyValue && j < size2) {
				j++;
			}
		} else {
			while (buffer1[i * IDCount1 + joinKey1] == keyValue && i < size1) {
				i++;
			}
		}
	}
}

void HashJoin::run(HashJoinArg* arg)
{
	//HashJoinArg* arg = (HashJoinArg*)m_ptrData;

//	/*
	if( arg->length2 > 100 && arg->length1 > 100 ) {
		vector<int> hashBucket1, hashBucket2;
		vector<int> prefixSum1, prefixSum2;
		ID hashKey;

		if ( arg->length1 < arg->length2 ) {
			hashKey = HashJoin::GetHashKey2(arg->length2);
		} else {
			hashKey = HashJoin::GetHashKey2(arg->length1);
		}

		HashJoin::BuildHashIndex(arg->buffer1->getBuffer() + arg->startPos1 * arg->buffer1->IDCount ,arg->joinKey1, hashKey, hashBucket1,
				prefixSum1, arg->length1, arg->buffer1->IDCount);
		HashJoin::BuildHashIndex(arg->buffer2->getBuffer() + arg->startPos2 * arg->buffer2->IDCount, arg->joinKey2, hashKey, hashBucket2,
				prefixSum2, arg->length2, arg->buffer2->IDCount);

		int start1, start2;
		int size1, size2;

		for ( size_t i = 0; i < hashKey; i++ ) {
			if ( hashBucket1[i] != 0 && hashBucket2[i] != 0 ) {
				if ( i == 0) {
					start1 = 0;
					start2 = 0;
				} else {
					start1 = prefixSum1[i - 1];
					start2 = prefixSum2[i - 1];
				}

				size1 = hashBucket1[i];
				size2 = hashBucket2[i];

				if( arg->flag2 != NULL)
					HashJoin::SortMergeJoin(arg->buffer1->getBuffer() + ( arg->startPos1 + start1 ) * arg->buffer1->IDCount,
							arg->buffer2->getBuffer() + ( arg->startPos2 + start2 ) * arg->buffer2->IDCount,
							arg->buffer1->IDCount, arg->buffer2->IDCount, arg->joinKey1, arg->joinKey2,
							size1, size2, arg->flag1 + arg->startPos1 + start1, arg->flag2 + arg->startPos2 + start2);
				else
					HashJoin::SortMergeJoin(arg->buffer1->getBuffer() + ( arg->startPos1 + start1 ) * arg->buffer1->IDCount,
							arg->buffer2->getBuffer() + ( arg->startPos2 + start2 ) * arg->buffer2->IDCount,
							arg->buffer1->IDCount, arg->buffer2->IDCount, arg->joinKey1, arg->joinKey2,
							size1, size2, arg->flag1 + arg->startPos1 + start1);
			}
		}

	} else {
//	*/
		if ( arg->flag2 != NULL )
			HashJoin::SortMergeJoin(arg->buffer1->getBuffer() + arg->startPos1 * arg->buffer1->IDCount,
					arg->buffer2->getBuffer() + arg->startPos2 * arg->buffer2->IDCount,
					arg->buffer1->IDCount, arg->buffer2->IDCount, arg->joinKey1, arg->joinKey2,
					arg->length1, arg->length2, arg->flag1 + arg->startPos1, arg->flag2 + arg->startPos2);
		else
			HashJoin::SortMergeJoin(arg->buffer1->getBuffer() + arg->startPos1 * arg->buffer1->IDCount,
					arg->buffer2->getBuffer() + arg->startPos2 * arg->buffer2->IDCount,
					arg->buffer1->IDCount, arg->buffer2->IDCount, arg->joinKey1, arg->joinKey2,
					arg->length1, arg->length2, arg->flag1 + arg->startPos1);
	}

	delete arg;
	return;
}

//used to hash id second time;
ID HashJoin::GetHashKey2(ID size)
{
	return (ID)pow(2,(int)log((double)size) / log(10.0) + 2);
}

ID HashJoin::GetHashKey(ID size)
{
	return (ID)pow(2,(int)log((double)size / log(10.0)));
}

ID HashJoin::HashFunction(ID id,ID hashKey)
{
	return id & hashKey;
}

void HashJoin::BuildHashIndex(ID* buffer,int joinKey, ID hashKey,
		vector<int>& hashBucket, vector<int>& prefixSum, int size, int IDCount)
{
	int i = 0;

	ID keyValue = 0;
	ID hashID = 0;

	hashBucket.resize(0);
	hashBucket.assign(hashKey,0);

	prefixSum.resize(0);
	prefixSum.assign(hashKey,0);

	hashKey--;

	while (i < size) {
		keyValue = buffer[i * IDCount + joinKey];
		hashID = HashFunction(keyValue, hashKey);

		hashBucket[hashID]++;

		i++;
	}

	//compute prefix sum;
	hashKey++;
	prefixSum[0] = 0;
	for ( size_t i = 1 ; i < hashKey; i++ ) {
		prefixSum[i] = hashBucket[i - 1] + prefixSum[i - 1];
	}

	//cout<<""
	//adjust the ids;
	ID *p = (ID*)malloc(sizeof(ID) * size * IDCount);
	//iBuffer = (ID*) entBuffer->getBuffer();
	int des;

	i = 0;
	hashKey--;
	while (i < size) {
		keyValue = buffer[i * IDCount + joinKey];
		hashID = HashFunction(keyValue, hashKey);

		des = prefixSum[hashID];

		//copy to the destination location.
		//for ( int j = 0; j < IDCount; j++)
		//	p[IDCount * des + j] = buffer[i * IDCount + j];
		memcpy(p + IDCount * des, buffer + i * IDCount, IDCount * sizeof(ID));

		prefixSum[hashID]++;
		i++;
	}

	//cppy the id back;
	memcpy(buffer, p , sizeof(ID) * size * IDCount);

	free(p);
}

void HashJoin::Join(EntityIDBuffer* entBuffer1, EntityIDBuffer* entBuffer2,
		int joinKey1, int joinKey2)
{
	joinKey1--;
	joinKey2--;

	entBuffer2->setSortKey(joinKey2);

	size_t size1, size2;
	size_t i;
	size1 = entBuffer1->getSize();
	size2 = entBuffer2->getSize();
	ID hashKey;

//	cout<<"iSize: "<<size1<<" jSize: "<<size2<<endl;

	if ( size1 < size2 ) {
		hashKey = GetHashKey(size2);
	} else {
		hashKey = GetHashKey(size1);
	}

	char* flagVector = (char*)malloc(size1);
	memset(flagVector,0,size1);

	char* flagVector2;
	if ( entBuffer2->IDCount >= 2) {
		flagVector2 = (char*)malloc(size2);
		memset(flagVector2,0,size2);
	} else {
		flagVector2 = NULL;
	}

	//TODO build hash index & partition the buffer;
	BuildHashIndex(entBuffer1->getBuffer(),joinKey1,hashKey, entBuffer1->hashBucket,
			entBuffer1->prefixSum, entBuffer1->getSize(), entBuffer1->IDCount);
	BuildHashIndex(entBuffer2->getBuffer(),joinKey2, hashKey, entBuffer2->hashBucket,
			entBuffer2->prefixSum, entBuffer2->getSize(), entBuffer2->IDCount);

	//TODO hash join;
	HashJoinArg* arg;
	//use thread to process one hash id;
	for ( i = 0; i < hashKey; i++ )
	{
		if ( entBuffer1->hashBucket[i] != 0 && entBuffer2->hashBucket[i] != 0) {
			arg = new HashJoinArg;
			arg->flag1 = flagVector;
			arg->flag2 = flagVector2;

			arg->buffer1 = entBuffer1;
			arg->buffer2 = entBuffer2;

			if( i == 0) {
				arg->startPos1 = 0;
				arg->startPos2 = 0;
			} else {
				arg->startPos1 = entBuffer1->prefixSum[i - 1];
				arg->startPos2 = entBuffer2->prefixSum[i - 1];
			}

			arg->length1 = entBuffer1->hashBucket[i];
			arg->length2 = entBuffer2->hashBucket[i];

			arg->joinKey1 = joinKey1;
			arg->joinKey2 = joinKey2;

			CThreadPool::getInstance().AddTask(boost::bind(&HashJoin::run, arg));
		}
	}

	CThreadPool::getInstance().Wait();

	entBuffer1->modifyByFlag(flagVector, 1);

	if ( flagVector2 != NULL)
		entBuffer2->modifyByFlag(flagVector2,1);


	free(flagVector);
	if ( flagVector2 != NULL)
		free(flagVector2);
}
