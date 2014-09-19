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

#include "EntityIDBuffer.h"
#include "MemoryBuffer.h"
#include <math.h>
#include <pthread.h>

EntityIDBuffer::EntityIDBuffer() {
	// TODO Auto-generated constructor stub
	buffer = (ID*) malloc(ENTITY_BUFFER_INIT_PAGE_COUNT * getpagesize());
	p = buffer;
	usedSize = 0;
	sizePerPage = getpagesize() / sizeof(ID);
	totalSize = ENTITY_BUFFER_INIT_PAGE_COUNT * sizePerPage;
	pos = 0;

	sorted = true;
	firstTime = true;
	IDCount = 1;
	sortKey = 0;
}
EntityIDBuffer::EntityIDBuffer(const EntityIDBuffer* otherBuffer) {
	usedSize = otherBuffer->usedSize;
	totalSize = otherBuffer->usedSize;
	IDCount = otherBuffer->IDCount;
	sortKey = otherBuffer->sortKey;
	sorted = otherBuffer->sorted;
	buffer = (ID*) malloc(otherBuffer->usedSize*sizeof(ID));
	memcpy(buffer, otherBuffer->buffer, usedSize * sizeof(ID));
}
EntityIDBuffer* EntityIDBuffer::operator =(const EntityIDBuffer *otherBuffer)
{
	if(this != otherBuffer) {
		usedSize = otherBuffer->usedSize;
		totalSize = otherBuffer->totalSize;
		IDCount = otherBuffer->IDCount;
		sortKey = otherBuffer->sortKey;
		sorted = otherBuffer->sorted;
		buffer = (ID*)realloc(buffer, totalSize * sizeof(ID));
		memcpy(buffer, otherBuffer->buffer, usedSize * sizeof(ID));
	}

	return this;
}

ID& EntityIDBuffer::operator [](const size_t index) {
	if(index > (usedSize / IDCount))
		return buffer[0];
	return buffer[index * IDCount + sortKey];
}

EntityIDBuffer::~EntityIDBuffer() {
	// TODO Auto-generated destructor stub
	if(buffer != NULL)
		free(buffer);
	buffer = NULL;
}


Status EntityIDBuffer::insertID(ID id)
{
	if( usedSize == totalSize )
	{
		buffer = (ID*)realloc((char*)buffer, totalSize * sizeof(ID) + ENTITY_BUFFER_INCREMENT_PAGE_COUNT * getpagesize());
		//memset((char*)(buffer + totalSize), 255, ENTITY_BUFFER_INCREMENT_PAGE_COUNT * getpagesize());

		p = buffer + totalSize;

		totalSize = totalSize + ENTITY_BUFFER_INCREMENT_PAGE_COUNT * sizePerPage;
		pos = 0;
	}

	p[pos] = id;
	usedSize++;
	pos++;

	return OK;
}

Status EntityIDBuffer::getID(ID& id, size_t _pos)
{
	if(_pos > usedSize)
		return ERROR;
	id = buffer[_pos];
	return OK;
}

Status EntityIDBuffer::sort()
{
	// TODO quicksort chunks
	int chunkCount = THREAD_NUMBER;
	
	size_t chunkSize =  getSize() / chunkCount;
	int i = 0;
	ID * p;
	for( i = 0; i < chunkCount; i++ )
	{
		p = buffer + i * chunkSize * IDCount;
		if( i == chunkCount -1 )
			CThreadPool::getInstance().AddTask(boost::bind(&SortTask::Run, p,getSize() - chunkSize * (chunkCount - 1), sortKey, IDCount));
		else
			CThreadPool::getInstance().AddTask(boost::bind(&SortTask::Run, p, chunkSize , sortKey, IDCount));
	}

	// TODO waiting for the task completing.
	CThreadPool::getInstance().Wait();

	//TODO merge the chunks into a buffer.
	ID* tempBuffer = (ID*)malloc(totalSize * 4);
	if(tempBuffer == NULL) {
		cout<<totalSize * 4<<endl;
		tempBuffer = (ID*)malloc(4096);
		size_t tempSize = 4096;
		while(tempSize < totalSize * 4) {
                	tempBuffer = (ID*)realloc(tempBuffer, tempSize + 4096);
                	tempSize = tempSize + 4096;
        	}
		assert(tempSize >= totalSize * 4);
	}

#ifdef DEBUG
	cout<<"totalsize: "<<totalSize<<endl;
#endif

	double j = 1;
	int slot;
	int max = (int)ceil( log((double)chunkCount) / log (2.0) );
	while (1) {
		slot = (int) pow(2, j);

		if (j > max)
			break;

		for (i = 0; i < chunkCount;i += slot) {
			if(i + slot == chunkCount) {
				CThreadPool::getInstance().AddTask(boost::bind(&EntityIDBuffer::merge, this, i * chunkSize, ( i + slot / 2 - 1) * chunkSize,
						(i + slot / 2) * chunkSize, getSize(), tempBuffer));
			} else {
				CThreadPool::getInstance().AddTask(boost::bind(&EntityIDBuffer::merge, this, i * chunkSize, ( i + slot / 2 - 1) * chunkSize,
						(i + slot / 2) * chunkSize, (i + slot) * chunkSize, tempBuffer));
			}
		}

		CThreadPool::getInstance().Wait();
		swapBuffer(tempBuffer);
		j++;
	}

	sorted = true;
	free(tempBuffer);
	return OK;
}

void EntityIDBuffer::swapBuffer(ID*& tempBuffer)
{
	ID* temp = buffer;
	buffer = tempBuffer;
	tempBuffer = temp;
}

void EntityIDBuffer::merge(int start1,int end1, int start2, int end2, ID* tempBuffer)
{
	ID* startPtr1 = buffer + start1 * IDCount;
	ID* endPtr1 = buffer + start2 * IDCount;
	ID* startPtr2 = buffer + start2 * IDCount;
	ID* endPtr2 = buffer + end2 * IDCount;

	ID* result = tempBuffer + start1 * IDCount;

	if ( IDCount == 1 ) {
		std::merge(startPtr1, endPtr1, startPtr2, endPtr2, result, SortTask::compareInt);
	} else {
		if ( sortKey == 0) {
			mergeBuffer(result, startPtr1, startPtr2, (start2 - start1), (end2 - start2) , 2, 0);
		} else if ( sortKey == 1) {
			mergeBuffer(result, startPtr1, startPtr2, (start2 - start1), (end2 - start2) , 2, 1);
		}
	}
}

Status EntityIDBuffer::mergeSingleBuffer(EntityIDBuffer* entbuffer ,EntityIDBuffer* entbuffer1,EntityIDBuffer* entbuffer2){
	return 	mergeSingleBuffer(entbuffer,entbuffer1->buffer,entbuffer2->buffer,entbuffer1->usedSize,entbuffer2->usedSize);
}

Status EntityIDBuffer::mergeSingleBuffer(EntityIDBuffer* entbuffer ,EntityIDBuffer* entbuffer1){
	EntityIDBuffer * entbuffer2 = new EntityIDBuffer();
	entbuffer2->setIDCount(1);
	entbuffer2->operator =(entbuffer);
	entbuffer->empty();
	return 	mergeSingleBuffer(entbuffer,entbuffer1->buffer,entbuffer2->buffer,entbuffer1->usedSize,entbuffer2->usedSize);
	delete entbuffer2;
}

Status EntityIDBuffer::mergeSingleBuffer(EntityIDBuffer* entbuffer ,ID* buffer1,
		ID* buffer2, size_t length1, size_t length2) {
	size_t i, j;
	i = 0;
	j = 0;
	ID keyValue;
	entbuffer->empty();
	while (i != length1 && j != length2) {
		keyValue = buffer1[i];

		if (buffer2[j] < keyValue) {
			entbuffer->insertID(buffer2[j]);
			j++;
		} else if (buffer2[j] == keyValue) {
			j++;
		}else {
			entbuffer->insertID(buffer1[i]);
			i++;
		}
	}

	while (i != length1) {
		entbuffer->insertID(buffer1[i]);
		i++;
	}

	while (j != length2) {
		entbuffer->insertID(buffer2[j]);
		j++;
	}

	return OK;
}

//.....IDCount.1.2...
void EntityIDBuffer::uniqe()
{
	ID* temp = (ID*)malloc(totalSize * sizeof(ID));

	ID record[2];
	size_t size = getSize();
	record[0] = record[1] = 0;
	
	size_t destPos = 0;
	
	for(size_t i = 0; i < size; i++) 
	{
		bool dumplicate = false;
		
		if(IDCount == 1) {
			if(record[0] == buffer[i]) {
				dumplicate = true;
			} else {
				record[0] = buffer[i];
			}
		} else {
			if(record[0] == buffer[2 * i] && record[1] == buffer[2 * i + 1]) {
				dumplicate = true;
			} else {
				record[0] = buffer[2 * i];
				record[1] = buffer[2 * i + 1];
			}
		}
		
		if(dumplicate == false) {
			for(int k = 0; k < IDCount; k++) {
				temp[destPos] = buffer[IDCount * i + k];
				destPos++;
			}
		}
	}
	
	delete buffer;
	buffer = temp;
	unique = true;
	usedSize = destPos - 1;
}

Status EntityIDBuffer::mergeBuffer(ID* destBuffer, ID* buffer1, ID* buffer2, size_t length1, size_t length2, int IDCount, int key)
{
	size_t i, j;
	i = 0; j = 0;
	int destPos = 0;

	ID keyValue;
	
	while ( i != length1 && j != length2 ) {
		keyValue = buffer1[i * IDCount + key];

		if(buffer2[j * IDCount + key] <= keyValue ) {
			for(int k = 0; k != IDCount; k++) {
				destBuffer[destPos * IDCount + k] = buffer2[j * IDCount + k];
			}
			destPos++;
			j++;
		} else {
			for(int k = 0; k != IDCount; k++) {
				destBuffer[destPos * IDCount + k] = buffer1[i * IDCount + k];
			}
			destPos++;
			i++;
		}
	}
	
	while(i != length1) {
		for(int k = 0; k < IDCount; k++) {
			destBuffer[destPos * IDCount + k] = buffer1[i * IDCount + k];
		}
		destPos++;
		i++;
	}
	
	while(j != length2) {
		for(int k = 0; k < IDCount; k++) {
			destBuffer[destPos * IDCount + k] = buffer2[j * IDCount + k];
		}
		destPos++;
		j++;
	}
	
	return OK;
}

void EntityIDBuffer::quickSort(ID* p, int size)
{
	CThreadPool::getInstance().AddTask(boost::bind(&SortTask::Run, p,size, sortKey, IDCount));
}

/*
 * get the id's position in the buffer.
 */
size_t EntityIDBuffer::getEntityIDPos(ID id)
{
	longlong low = 0, high = getSize() - 1;
	longlong mid;

	while( low <= high ) {
		mid = low + (high - low) / 2;
		if ( buffer[mid * IDCount + sortKey] == id) {
			while ( mid != size_t(-1) && buffer[mid * IDCount + sortKey] == id) mid--;
			mid++;
			return (size_t)mid;
		}
		else if ( buffer[mid * IDCount + sortKey] > id) high = mid - 1;
		else low = mid + 1;
	}

	return size_t(-1);
}

void EntityIDBuffer::print()
{
	size_t total = getSize();
	size_t i;

	ID* p = buffer;

	int k;

	for( i = 0 ; i != total; i++)
	{
		for( k = 0; k < IDCount; k++)
		{
			cout<<p[i * IDCount + k]<<" ";
		}
		cout<<endl;
	}
}


//TODO merge join;
Status EntityIDBuffer::mergeIntersection(EntityIDBuffer* entBuffer, char* flags, ID joinKey) {
	joinKey--;

	size_t iSize, jSize;
	size_t i = 0, j = 0;
	size_t iPos, jPos, pos;

	ID * iBuffer, *jBuffer;

	iSize = usedSize / IDCount;
	jSize = entBuffer->getSize();

	iPos = 0;
	jPos = 0;
	pos = 0;

	iBuffer = (ID*) buffer;
	jBuffer = (ID*) entBuffer->getBuffer();

	ID keyValue;

	while (i < iSize && j < jSize) {
		keyValue = iBuffer[i * IDCount + joinKey];

		while (jBuffer[j] < keyValue && j < jSize) {
			j++;
		}

		if (jBuffer[j] == keyValue) {
			while (iBuffer[i * IDCount + joinKey] == keyValue && i < iSize) {
				flags[i]++;
				i++;
			}

			while (jBuffer[j] == keyValue && j < jSize) {
				j++;
			}
		} else {
			while (iBuffer[i * IDCount + joinKey] == keyValue && i < iSize) {
				i++;
			}
		}
	}

	return OK;
}

/**
 * get the maximun and minimun id in the buffer.
 */
void EntityIDBuffer::getMinMax(ID& min, ID& max)
{
	size_t size = getSize();
	min = buffer[sortKey];
	max = buffer[(size - 1) * IDCount + sortKey];
/*
	int size = getSize();

	for( int i = 0; i < size; i++ ) {
		if ( buffer[i * IDCount + sortKey] < min ) {
			min = buffer[i * IDCount + sortKey];
		}

		if ( buffer[i * IDCount + sortKey] > max) {
			max = buffer[i * IDCount + sortKey];
		}
	}
*/
}

Status EntityIDBuffer::intersection(EntityIDBuffer* entBuffer, char* flags, ID joinKey1, ID joinKey2)
{
	joinKey1--;
	joinKey2--;

	ID* temp2 = (ID*) malloc(getpagesize());

	size_t iSize, jSize, _size2;
	size_t i = 0, j = 0;
	size_t pos2;
	ID * iBuffer, *jBuffer;

	size_t totalPerPage =  getpagesize() / 4;

	ID IDCount2 = entBuffer->IDCount;

	iSize = getSize();
	jSize = entBuffer->getSize();

	pos2 = 0;

	iBuffer = (ID*) buffer;
	jBuffer = (ID*) entBuffer->getBuffer();

	_size2 = 0;

	ID keyValue;

	unsigned int k;

	int sameCount = 0;
	int sameCountJBuffer = 0;

	while (i < iSize && j < jSize) {
		keyValue = iBuffer[i * IDCount + joinKey1];

		while (jBuffer[j * IDCount2 + joinKey2] < keyValue && j < jSize ) {
			j++;
		}

		if (jBuffer[j * IDCount2 + joinKey2] == keyValue) {
			while (iBuffer[i * IDCount + joinKey1] == keyValue && i < iSize) {
				sameCount++;
				flags[i]++;
				i++;

			}

			while (jBuffer[j * IDCount2 + joinKey2] == keyValue && j < jSize) {
				if (pos2 == totalPerPage) {
					memcpy(entBuffer->getBuffer() + _size2, temp2, getpagesize());
					_size2 += pos2;
					pos2 = 0;
				}

				for (k = 0; k < IDCount2; k++) {
					temp2[pos2] = jBuffer[j * IDCount2 + k];
					pos2++;
				}

				sameCountJBuffer++;
				j++;

			}
		}else{
			while (iBuffer[i * IDCount + joinKey1] == keyValue && i < iSize) {
				i++;
			}
		}
	}


	memcpy(entBuffer->getBuffer() + _size2,temp2, pos2 * 4);
	_size2 += pos2;
	entBuffer->usedSize = _size2;
	free(temp2);

	return OK;
}

int SortTask::Run(ID* p, size_t length, int sortKey, int IDCount)
{
	if(IDCount == 1) {
		qsort(&p[0], length, sizeof(ID), qcompareInt);
	} else if ( IDCount == 2) {
		if ( sortKey == 0) {
			qsort((int64_t*)&p[0], length, sizeof(int64_t), qcompareLongByFirst32);
		} else if ( sortKey == 1) {
			qsort((int64_t*)&p[0], length, sizeof(int64_t), qcompareLongBySecond32);
		}
	}

	return 0;
}

int SortTask::qcompareInt(const void* a, const void* b)
{
	return *((const ID*)a) - *((const ID*)b);
}

int SortTask::qcompareLongByFirst32(const void* a, const void* b)
{
	const ID* ptr1 = (const ID*)a;
	const ID* ptr2 = (const ID*)b;

	return ptr1[0] - ptr2[0];
}

int SortTask::qcompareLongBySecond32(const void* a, const void* b)
{
	const ID* ptr1 = (const ID*)a;
	const ID* ptr2 = (const ID*)b;

	return ptr1[1] - ptr2[1];
}

bool SortTask::compareInt(ID a, ID b) {
	return a < b;
}

bool SortTask::compareLongByFirst32(int64_t a, int64_t b)
{
	ID* ptr1 = (ID*)&a;
	ID* ptr2 = (ID*)&b;
	
	return ptr1[0] < ptr2[0];
}

bool SortTask::compareLongBySecond32(int64_t a, int64_t b)
{
	ID* ptr1 = (ID*)&a;
	ID* ptr2 = (ID*)&b;

	return ptr1[1] < ptr2[1];
}

Status EntityIDBuffer::modifyByFlag(char* flag,int No)
{
	size_t totalPerPage = getpagesize() / 4;

	ID* temp = (ID*) malloc(getpagesize());
	ID* p = buffer;

	size_t size = this->getSize();

	size_t j;
	size_t pos = 0;
	int n;
	size_t writeSize = 0;


	for ( j= 0; j < size; j++)
	{
		if ( flag[j] == No)
		{
			if ( pos == totalPerPage) {
				memcpy(buffer + writeSize,temp, getpagesize());
				writeSize += pos;
				pos = 0;
			}
			for( n = 0; n < IDCount; n++) {
				temp[pos] = p[j * IDCount + n];
				pos++;
			}
		}
	}

	memcpy(buffer + writeSize, temp, pos * 4);
	writeSize += pos;
	usedSize = writeSize;

	free(temp);

	return OK;
}

bool EntityIDBuffer::isInBuffer(ID res[])
{
	size_t pos;
	pos = this->getEntityIDPos(res[0]);

	int varPos = (sortKey == 1 ? -1 : 1);
	if ( pos == size_t(-1))
		return false;
	else {
		while( buffer[pos * IDCount + sortKey] == res[0]) {
			if ( buffer[pos * IDCount + sortKey + varPos] == res[1])
				return true;
			pos++;
		}
	}

	return false;
}

//the buffer must has been sorted by the sortKey.
ID EntityIDBuffer::getMaxID()
{
	size_t size = getSize();

	ID maxID = buffer[size * IDCount + sortKey];

	return maxID;
}


///merge the XTemp and XYTemp into a buffer;
Status EntityIDBuffer::mergeBuffer(EntityIDBuffer* XTemp, EntityIDBuffer* XYTemp)
{
	size_t totalSize = ( XTemp->getSize() + XYTemp->getSize() ) * XTemp->IDCount;
	
	if( buffer != NULL)
		free(buffer);
	buffer = NULL;

	int pageCount = (int)ceil((double)totalSize / (double)sizePerPage);
	this->usedSize = totalSize;
	this->totalSize = pageCount * sizePerPage;

	buffer = (ID*)realloc(buffer, this->totalSize * sizeof(ID));

	if(buffer == NULL) {
		buffer = (ID*)malloc(getpagesize());
		for(int i = 1; i < pageCount; i++) {
			buffer = (ID*)realloc(buffer, (i + 1)*getpagesize());
			if(buffer == NULL)
				MessageEngine::showMessage("EntityIDBuffer::mergeBuffer, realloc error!", MessageEngine::ERROR);
		}
	}

	if (XTemp->getSize() == 0) {
		//buffer = XYTemp->buffer;
		//XYTemp->buffer = NULL;
		memcpy(buffer, XYTemp->buffer, usedSize * sizeof(ID));
		return OK;
	}

	if (XYTemp->getSize() == 0) {
		//buffer = XTemp->buffer;
		//XTemp->buffer = NULL;
		memcpy(buffer, XTemp->buffer, usedSize * sizeof(ID));
		return OK;
	}

	ID* start1 = XTemp->getBuffer();
	ID* end1 = start1 + XTemp->getSize() * XTemp->IDCount;

	ID* start2 = XYTemp->getBuffer();
	ID* end2 = start2 + XYTemp->getSize() * XYTemp->IDCount;

	if ( IDCount == 1) {
		std::merge(start1, end1, start2, end2, buffer, SortTask::compareInt);
	} else {
		if ( sortKey == 0 ) {
			mergeBuffer(buffer, start1, start2, XTemp->getSize(), XYTemp->getSize(), 2, 0);
		} else if ( sortKey == 1) {
			mergeBuffer(buffer, start1, start2, XTemp->getSize(), XYTemp->getSize(), 2, 1);
		}
	}
	
	return OK;
}

Status EntityIDBuffer::appendBuffer(const EntityIDBuffer *otherBuffer)
{
	return appendBuffer1(otherBuffer->getBuffer(), otherBuffer->usedSize);
}

Status EntityIDBuffer::appendBuffer(const ID *buf, size_t size)
{
	if(usedSize + size > totalSize) {
		buffer = (ID*)realloc(buffer, (usedSize + size) * sizeof(ID));
		if(buffer == NULL)
			perror(__FUNCTION__);
		return ERROR;
	}
	usedSize = usedSize + size;
	memcpy(buffer + usedSize, buf, size * sizeof(ID));
	return OK;
}

Status EntityIDBuffer::appendBuffer1(const ID *buf, size_t size) {
	while (usedSize + size > totalSize) {
		buffer = (ID*) realloc((char*) buffer, totalSize * sizeof(ID)
				+ ENTITY_BUFFER_INIT_PAGE_COUNT * MemoryBuffer::pagesize);
		totalSize = totalSize + ENTITY_BUFFER_INIT_PAGE_COUNT * sizePerPage;
	}
	memcpy(buffer + usedSize, buf, size * sizeof(ID));
	usedSize += size;
	return OK;
}

Status EntityIDBuffer::swapBuffer(EntityIDBuffer* &buffer1,EntityIDBuffer* &buffer2){
	EntityIDBuffer * temp = buffer1;
	buffer1 = buffer2;
	buffer2 = temp;
	return OK;
}
