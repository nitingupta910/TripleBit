#ifndef SYNCHRONOUSBUFFER_H_
#define SYNCHRONOUSBUFFER_H_

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

#define SYNCHRONOUSBUFFER_DEBUG 1

class MemoryBuffer;

#include "TripleBit.h"

#include <pthread.h>

#include <iostream>

using namespace std;

class SynchronousBuffer {
private:
	MemoryBuffer* buffer;

	//for synchronous access the buffer
	pthread_mutex_t bufferLock;
	pthread_cond_t  bufferNotEmpty;
	pthread_cond_t  bufferFull;
	pthread_cond_t  bufferNotFull;

	char* base;
	unsigned int readPos;
	unsigned int writePos;

	unsigned int pageSize;
	unsigned int usedSize;
	unsigned int remainderSize;

	bool finish; //used to identify whether writing is finished;
public:
	SynchronousBuffer();
	Status 	MemoryCopy(void* src, size_t length);
	Status 	MemoryGet(void* dest, size_t length);
	void	SetFinish() { finish = true; }
	virtual ~SynchronousBuffer();

private:
	bool 	IsBufferFull(size_t length) { return (writePos + length) % pageSize == readPos; }
	bool 	IsBufferEmpty() { return (readPos == writePos); }
};

#endif /* SYNCHRONOUSBUFFER_H_ */
