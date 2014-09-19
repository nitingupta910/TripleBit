#ifndef LINEHASHINDEX_H_
#define LINEHASHINDEX_H_

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

class MemoryBuffer;
class ChunkManager;
class MMapBuffer;

#include "TripleBit.h"
#include "EntityIDBuffer.h"

class LineHashIndex {
public:
	struct Point{
		ID x;
		ID y;
	};

	enum IndexType { SUBJECT_INDEX, OBJECT_INDEX};
private:
	MemoryBuffer* idTable;
	MemoryBuffer* offsetTable;
	ID* idTableEntries;
	ID* offsetTableEntries;
	ChunkManager& chunkManager;
	IndexType indexType;
	unsigned int tableSize;
	unsigned lineNo;

	//line parameters;
	double upperk[4];
	double upperb[4];
	double lowerk[4];
	double lowerb[4];

	ID startID[4];
private:
	void insertEntries(ID id, unsigned offset);
	int searchChunk(ID id);
	bool buildLine(int startEntry, int endEntry, int lineNo);
public:
	LineHashIndex(ChunkManager& _chunkManager, IndexType type);
	Status buildIndex(unsigned chunkType);
	Status getOffsetByID(ID id, unsigned& offset, unsigned typeID);
	Status getFirstOffsetByID(ID id, unsigned& offset, unsigned typeID);
	Status getYByID(ID id,EntityIDBuffer* entBuffer,unsigned typeID);
	void save(MMapBuffer*& indexBuffer);
	virtual ~LineHashIndex();
private:
	bool isBufferFull();
public:
	static LineHashIndex* load(ChunkManager& manager, IndexType type, char* buffer, size_t& offset);
	static void unload( char* buffer, size_t& offset);
};

#endif /* LINEHASHINDEX_H_ */
