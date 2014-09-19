#ifndef SORTMERGEJOIN_H_
#define SORTMERGEJOIN_H_

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

class EntityIDBuffer;

#include "../TripleBit.h"
#include "../ThreadPool.h"

struct SortMergeJoinArg;

class SortMergeJoin {
	//CThreadPool* pool;
	ID* temp1;
	ID* temp2;
public:
	SortMergeJoin();
	///Execute join operation
	void Join(EntityIDBuffer* entBuffer1, EntityIDBuffer* entBuffer2, int joinKey1, int joinKey2, bool secondModify = true);
	// do the merge operation;
	int Merge(SortMergeJoinArg* arg);
	// need to modify buffer2;
	void Merge1(EntityIDBuffer* entBuffer1, EntityIDBuffer* entBuffer2, int joinKey1, int joinKey2);
	// not need to modify buffer2;
	void Merge2(EntityIDBuffer* entBuffer1, EntityIDBuffer* entBuffer2, int joinKey1, int joinKey2);
	virtual ~SortMergeJoin();
};

struct SortMergeJoinArg
{
	ID* buffer1, *buffer2;
	int length1, length2;
	char* flag1, *flag2;
	int IDCount1, IDCount2;
	int joinKey1, joinKey2;
	
	SortMergeJoinArg(ID* _buffer1, ID* _buffer2, int _length1, int _length2, char* flag1, char* flag2, int IDCount1, int IDCount2,
		int joinKey1, int joinKey2) : buffer1(_buffer1), buffer2(_buffer2), length1(_length1), length2(_length2),
		flag1(flag1), flag2(flag2), IDCount1(IDCount1), IDCount2(IDCount2),joinKey1(joinKey1),joinKey2(joinKey2){}
};

#endif /* SORTMERGEJOIN_H_ */
