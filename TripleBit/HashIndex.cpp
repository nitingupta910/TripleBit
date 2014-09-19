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

#include "HashIndex.h"
#include "BitmapBuffer.h"
#include "MemoryBuffer.h"
#include "MMapBuffer.h"

HashIndex::HashIndex(ChunkManager& _chunkManager, IndexType type) : chunkManager(_chunkManager), type(type) {
	// TODO Auto-generated constructor stub
	//how to init two-dim vector
	hashTable = NULL;
	hashTableSize = 0;
	hashTableEntries = NULL;
	firstValue = 0;
//	secondaryHashTable = NULL;
//	secondaryHashTableSize = 0;
}

HashIndex::~HashIndex() {
	// TODO Auto-generated destructor stub
	if(hashTable != NULL) {
		//delete temp files；
		//hashTable->discard();
		delete hashTable;
		hashTable = NULL;
	}
//	if(secondaryHashTable != NULL)
//		delete secondaryHashTable;
	hashTableEntries = NULL;
	hashTableSize = 0;
//	secondaryHashTableSize = 0;
}

Status HashIndex::hashInsert(ID id, unsigned int offset) {
	id = id - firstValue;
	for(; id / HASH_RANGE >= hashTableSize; ) {
		hashTableEntries[hashTableSize] = 0;
		hashTable->resize(HASH_CAPACITY_INCREASE);
		hashTableEntries = (ID*)hashTable->getBuffer();
		hashTableSize += HASH_CAPACITY_INCREASE / sizeof(unsigned);
//		hashTableEntries[hashTableSize] = firstValue;
	}

	if(id >= nextHashValue) {
		hashTableEntries[id / HASH_RANGE] = offset;
		while(nextHashValue <= id) nextHashValue += HASH_RANGE;
	}

	return OK;
}

void HashIndex::insertFirstValue(unsigned value)
{
	firstValue = (value / HASH_RANGE) * HASH_RANGE;
	//ID* hashTableEntries = (ID*)hashTable->getBuffer();
//	hashTableEntries[hashTableSize] = firstValue;
}

static void getTempIndexFilename(string& filename, int pid, unsigned type, unsigned chunkType)
{
	filename.clear();
	filename.append(DATABASE_PATH);
	filename.append("tempIndex_");
	char temp[4];
	sprintf(temp, "%d", pid);
	filename.append(temp);
	filename.append("_");
	sprintf(temp, "%d", type);
	filename.append(temp);
	filename.append("_");
	sprintf(temp, "%d", chunkType);
	filename.append(temp);
}

Status HashIndex::buildIndex(unsigned chunkType)
{
	if(hashTable == NULL) {
		string filename;
		getTempIndexFilename(filename, chunkManager.meta->pid, chunkManager.meta->type, chunkType);
		hashTable = new MemoryBuffer(HASH_CAPACITY);
		hashTableSize = hashTable->getSize() / sizeof(unsigned) - 1;
		hashTableEntries = (ID*)hashTable->getBuffer();
		nextHashValue = 0;
	}

	const uchar* begin, *limit, *reader;
	ID x,y;
	ID startID = 0;
	unsigned _offset = 0;
	if(chunkType == 1) {
		begin = reader = chunkManager.getStartPtr(1);
		if(chunkManager.getStartPtr(chunkType) == chunkManager.getEndPtr(chunkType))
			return OK;

		x = 0;
		reader = Chunk::readXId(reader, x);

		insertFirstValue(x);
		hashInsert(x, 1);
		while(startID <= x) startID += HASH_RANGE;
		reader = Chunk::skipId(reader, 1);

		limit = chunkManager.getEndPtr(1);
		while(reader < limit) {
			x = 0;
			_offset = reader - begin + 1;
			reader = Chunk::readXId(reader, x);
			if(x >= startID) {
				hashInsert(x, _offset);
				while(startID <= x) startID += HASH_RANGE;
			} else if(x == 0)
				break;
			reader = Chunk::skipId(reader, 1);
		}
	} 

	if(chunkType == 2) {
		if(chunkManager.getStartPtr(chunkType) == chunkManager.getEndPtr(chunkType))
			return OK;
		begin = reader = chunkManager.getStartPtr(2);

		x = y = 0;
		reader = Chunk::readXId(reader, x);
		reader = Chunk::readYId(reader, y);
		insertFirstValue(x + y);
		hashInsert(x + y, 1);
		while(startID <= x + y) startID += HASH_RANGE;

		limit = chunkManager.getEndPtr(2);
		while(reader < limit) {
			x = y = 0;
			_offset = reader - begin + 1;
			reader = Chunk::readXId(reader, x);
			reader = Chunk::readYId(reader, y);

			if(x + y >= startID) {
				hashInsert(x + y, _offset);
				while(startID <= x + y) startID += HASH_RANGE;
			} else if( x + y == 0) {
				break;
			}
		}
	}

	return OK;
}

Status HashIndex::getOffsetByID(ID id, unsigned& offset, unsigned typeID)
{
	unsigned pBegin = hash(id);
	unsigned pEnd = next(id);

	const uchar* beginPtr = NULL, *reader = NULL;
	int low, high, mid, lastmid = 0;
	ID x,y;

	if(chunkManager.getTrpileCount(typeID) == 0)
		return NOT_FOUND;

	if (typeID == 1) {
		if(pBegin != 0) {
			low = pBegin - 1;
			high = pEnd;
		} else {
			low = 0;
			high = offset = pEnd - 1;
			return OK;
		}
		reader = chunkManager.getStartPtr(1) + low;
		beginPtr = chunkManager.getStartPtr(1);
		Chunk::readXId(reader, x);

		if ( x == id ) {
			offset = low;
			return OK;
		} else if(x > id)
			return OK;

		while(low <= high) {
			x = 0;
			lastmid = mid = low + (high - low) / 2;
			reader = Chunk::skipBackward(beginPtr + mid);
			mid = reader - beginPtr;
			Chunk::readXId(reader, x);

			if(x == id){
				while(mid >= (int)MemoryBuffer::pagesize) {
					x = 0;
					lastmid = mid;
					mid = mid - MemoryBuffer::pagesize;
					reader = Chunk::skipBackward(beginPtr + mid);
					mid = beginPtr - reader;
					if(mid <= 0) {
						offset = 0;
						return OK;
					}
					Chunk::readXId(reader, x);
					if(x < id)
						break;
				}
				//mid = lastmid;
				if(mid < (int)MemoryBuffer::pagesize)
					mid = 0;
				while(mid <= lastmid) {
					x = 0;
					reader = Chunk::readXId(beginPtr + mid, x);
					reader = Chunk::skipId(reader, 1);
					if(x >= id) {
						offset = mid;
						return OK;
					}
					mid = reader - beginPtr;
					if(mid == lastmid) {
						offset = mid;
						return OK;
					}
				}
				return OK;
			} else if ( x > id ) {
				high = mid - 1;
			} else {
				low = mid + 1;
			}
		}
	}

	if (typeID == 2) {
		if(pBegin != 0) {
			low = pBegin - 1;
			high = pEnd;
		} else {
			low = 0;
			high = offset = pEnd - 1;
			return OK;
		}
		reader = chunkManager.getStartPtr(2) + low;
		beginPtr = chunkManager.getStartPtr(2);

		reader = Chunk::readXId(reader, x);
		reader = Chunk::readYId(reader, y);
		if ( x + y == id ) {
			offset = low;
			return OK;
		}

		if(x + y > id)
			return OK;

		while(low <= high) {
			x = 0;
			mid = low + (high - low) / 2;//(low + high) / 2;
			reader = Chunk::skipBackward(beginPtr + mid);
			lastmid = mid = reader - beginPtr;
			reader = Chunk::readXId(reader, x);
			reader = Chunk::readYId(reader, y);
			if(x + y == id){
				while(mid >= (int)MemoryBuffer::pagesize) {
					x = 0; y = 0;
					lastmid = mid;
					mid = mid - MemoryBuffer::pagesize;
					reader = Chunk::skipBackward(beginPtr + mid);
					mid = beginPtr - reader;
					if (mid <= 0) {
						offset = 0;
						return OK;
					}
					Chunk::readYId(Chunk::readXId(reader, x), y);
					if (x + y < id)
						break;
				}

				if(mid < (int)MemoryBuffer::pagesize)
					mid = 0;
				while( mid <= lastmid) {
					x = y = 0;
					reader = Chunk::readYId(Chunk::readXId(beginPtr + mid, x), y);
					if( x + y >= id) {
						offset = mid;
						return OK;
					}

					mid = reader - beginPtr;
					if(mid == lastmid) {
						offset = mid;
						return OK;
					}
				}
			} else if ( x + y > id ) {
				high = mid - 1;
			} else {
				low = mid + 1;
			}
		}
	}
	if(mid <= 0)
		offset = 0;
	else //if not found, offset is the first id which is bigger than the given id.
		offset = Chunk::skipBackward(beginPtr + mid) - beginPtr;	
	
	return OK;
}

unsigned HashIndex::hash(ID id)
{
	id = id - firstValue;
	if((int)id < 0)
		return 0;

	unsigned firstHash = id / HASH_RANGE;
	if(firstHash >= hashTableSize)
		return hashTableEntries[hashTableSize - 1];

	return hashTableEntries[firstHash];
}

unsigned HashIndex::next(ID id)
{
	id = id - firstValue;
	if((int)id < 0)
		return 1;
	unsigned firstHash;
	firstHash = id / HASH_RANGE + 1;
	if(firstHash >= hashTableSize)
		return hashTableEntries[hashTableSize - 1];

	while(hashTableEntries[firstHash] == 0) {
		firstHash++;
	}

	return hashTableEntries[firstHash];
}

char* writeData(char* writer, unsigned int data)
{
	memcpy(writer, &data, 4);
	return writer+4;
}

const char* readData(const char* reader, unsigned int& data)
{
	memcpy(&data, reader, 4);
	return reader+4;
}

void HashIndex::save(MMapBuffer*& buffer)
{
//	hashTable->flush();
	//size_t size = buffer->getSize();
	char* writeBuf;

	if(buffer == NULL) {
		buffer = MMapBuffer::create(string(string(DATABASE_PATH) + "BitmapBuffer_index").c_str(), hashTable->getSize() + 4);
		writeBuf = buffer->get_address();
	} else {
		size_t size = buffer->getSize();
		writeBuf = buffer->resize(hashTable->getSize() + 4) + size;
	}

//	assert(hashTableSize == hashTable->getSize() / 4 - 1);
	hashTableEntries[nextHashValue / HASH_RANGE] = firstValue;
	*(ID*)writeBuf = (nextHashValue / HASH_RANGE) * 4; writeBuf += 4;
	memcpy(writeBuf, (char*)hashTableEntries, (nextHashValue / HASH_RANGE) * 4);
	buffer->flush();

//	cout<<firstValue<<endl;
	delete hashTable;
	hashTable = NULL;
}

HashIndex* HashIndex::load(ChunkManager& manager, IndexType type, char* buffer, unsigned int& offset)
{
	HashIndex* index = new HashIndex(manager, type);
	size_t size = ((ID*)(buffer + offset))[0];
	index->hashTableEntries = (ID*)(buffer + offset + 4);
	index->hashTableSize = size / 4 - 1;
	index->firstValue = index->hashTableEntries[index->hashTableSize];

//	cout<<index->firstValue<<endl;
	offset += size + 4;
	return index;
}
