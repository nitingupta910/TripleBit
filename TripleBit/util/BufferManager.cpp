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

#include "BufferManager.h"
#include "../EntityIDBuffer.h"

BufferManager* BufferManager::instance = NULL;

BufferManager::BufferManager()
{
	// TODO Auto-generated constructor stub
	for (int i = 0; i < INIT_BUFFERS; i++) {
		EntityIDBuffer* buffer = new EntityIDBuffer;
		bufferPool.push_back(buffer);
		cleanBuffer.push_back(buffer);
	}
	usedBuffer.clear();
	bufferCnt = INIT_BUFFERS;
}

BufferManager::~BufferManager()
{
	// TODO Auto-generated destructor stub
}

bool BufferManager::expandBuffer()
{
	for (int i = 0; i < INCREASE_BUFFERS; i++) {
		EntityIDBuffer* buffer = new EntityIDBuffer;
		if (buffer == NULL) {
			bufferCnt += i;
			return false;
		}
		bufferPool.push_back(buffer);
		cleanBuffer.push_back(buffer);
	}

	bufferCnt += INCREASE_BUFFERS;
	return true;
}

EntityIDBuffer* BufferManager::getNewBuffer()
{
	if (usedBuffer.size() == bufferPool.size()) {
		if (expandBuffer() == false)
			return NULL;
	}
	EntityIDBuffer* buffer = cleanBuffer.front();
	cleanBuffer.erase(cleanBuffer.begin());;
	usedBuffer.push_back(buffer);

	return buffer;
}

void BufferManager::destroyBuffers()
{
	for (size_t i = 0; i < bufferPool.size(); i++) {
		delete bufferPool[i];
	}

	usedBuffer.clear();
	cleanBuffer.clear();
}

Status BufferManager::freeBuffer(EntityIDBuffer* buffer)
{
	vector<EntityIDBuffer*>::iterator iter;
	iter = find(usedBuffer.begin(), usedBuffer.end(), buffer);
	if (iter != usedBuffer.end()) {
		usedBuffer.erase(iter);
		cleanBuffer.push_back(*iter);
		(*iter)->empty();
		return OK;
	} else {
		return NOT_FOUND;
	}
}

Status BufferManager::reserveBuffer()
{
	usedBuffer.clear();
	cleanBuffer.clear();
	int i;
	for (i = 0; i < INIT_BUFFERS; i++) {
		bufferPool[i]->empty();
		cleanBuffer.push_back(bufferPool[i]);
	}

	vector<EntityIDBuffer*>::iterator iter = bufferPool.begin() + i;
	vector<EntityIDBuffer*>::iterator start = iter;
	for (; iter != bufferPool.end(); iter++) {
		delete *iter;
		*iter = NULL;
	}
	bufferPool.erase(start,iter);


	return OK;
}

BufferManager* BufferManager::getInstance()
{
	if (instance == NULL) {
		instance = new BufferManager;
	}

	return instance;
}
