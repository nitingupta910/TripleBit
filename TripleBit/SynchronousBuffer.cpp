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

#include "SynchronousBuffer.h"
#include "MemoryBuffer.h"

SynchronousBuffer::SynchronousBuffer() {
	// TODO Auto-generated constructor stub
	pthread_cond_init(&bufferNotEmpty,NULL);
	pthread_cond_init(&bufferFull,NULL);
	pthread_cond_init(&bufferNotFull,NULL);
	pthread_mutex_init(&bufferLock,NULL);

	this->buffer = new MemoryBuffer(1);

	this->pageSize = getpagesize();
	this->usedSize = 0;
	this->remainderSize = pageSize - usedSize;

	this->base = buffer->getBuffer();
	readPos = writePos = 0;

	this->finish = false;
}

SynchronousBuffer::~SynchronousBuffer() {
	// TODO Auto-generated destructor stub
	delete buffer;
}

Status SynchronousBuffer::MemoryCopy(void* src, size_t length)
{
	//TODO copy something to the memory;
	int rtn;
	if((rtn = pthread_mutex_lock(&bufferLock)) != 0)
		fprintf(stderr, "pthread_mutex_lock %d", rtn), exit(1);

	while(IsBufferFull(length)){
		if ((rtn = pthread_cond_wait(&bufferNotFull,&bufferLock)) != 0)
			fprintf(stderr, "pthread_cond_wait %d", rtn), exit(1);
	}

	memcpy(base+writePos,src,length);
	writePos = (writePos + length) % pageSize;

	if ((rtn = pthread_cond_broadcast(&bufferNotEmpty)) != 0)
		fprintf(stderr, "pthread_cond_signal %d", rtn), exit(1);

	pthread_mutex_unlock(&bufferLock);

	return OK;
}

Status SynchronousBuffer::MemoryGet(void* dest, size_t length)
{
	// TODO copy something from buffer to dest

	int rtn;
	if ((rtn = pthread_mutex_lock(&bufferLock)) != 0)
		fprintf(stderr, "pthread_mutex_lock %d", rtn), exit(1);

	while (IsBufferEmpty()) {
		if( finish == true){
			pthread_mutex_unlock(&bufferLock);
			return FINISH_READ;
		}
		if ((rtn = pthread_cond_wait(&bufferNotEmpty, &bufferLock)) != 0)
			fprintf(stderr, "pthread_cond_wait %d", rtn), exit(1);
	}

	memcpy((char*)dest, (const char*)(base + readPos), length);
	readPos = (readPos + length) % pageSize;

	if ((rtn = pthread_cond_broadcast(&bufferNotFull)) != 0)
		fprintf(stderr, "pthread_cond_signal %d", rtn), exit(1);

	pthread_mutex_unlock(&bufferLock);

	return OK;
}


