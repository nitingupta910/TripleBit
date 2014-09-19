#ifndef HASHINDEX_H_
#define HASHINDEX_H_

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

class ChunkManager;
class MemoryBuffer;
class MMapBuffer;

#include "TripleBit.h"

class HashIndex {
public:
	enum IndexType { SUBJECT_INDEX, OBJECT_INDEX};
private:
	/// store the chunks' position and the offset in chunk
	MemoryBuffer* hashTable;
	ID* hashTableEntries;
	//MMapBuffer* secondaryHashTable;
	/// the current size of hash index;
	unsigned int hashTableSize;
	//unsigned int secondaryHashTableSize;

	ChunkManager& chunkManager;
	/// index type;
	IndexType type;

	unsigned nextHashValue;// lastSecondaryHashTableOffset, secondaryHashTableOffset;
	unsigned firstValue;
	//ID* secondaryHashTableWriter;
protected:
	void insertFirstValue(unsigned value);
public:
	HashIndex(ChunkManager& _chunkManager, IndexType type);
	virtual ~HashIndex();
	/// build hash index; chunkType: 1 or 2
	Status buildIndex(unsigned chunkType);
	/// search the chunk and offset in chunk by id; typeID 1 or 2
	Status getOffsetByID(ID id, unsigned& offset, unsigned typeID);
	void save(MMapBuffer*& buffer);
public:
	static HashIndex* load(ChunkManager& manager, IndexType type, char* buffer, unsigned int& offset);
private:
	/// insert a record into index; position is the position of chunk in chunks vector.
	Status hashInsert(ID id, unsigned int offset);
	unsigned hash(ID id);
	unsigned next(ID id);
};

#endif /* HASHINDEX_H_ */
