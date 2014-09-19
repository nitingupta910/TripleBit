#ifndef STATISTICSBUFFER_H_
#define STATISTICSBUFFER_H_

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

class HashIndex;
class EntityIDBuffer;
class MMapBuffer;

#include "TripleBit.h"

class StatisticsBuffer {
public:
	enum StatisticsType { SUBJECT_STATIS, OBJECT_STATIS, SUBJECTPREDICATE_STATIS, OBJECTPREDICATE_STATIS };
	StatisticsBuffer();
	virtual ~StatisticsBuffer();
	/// add a statistics record;
	virtual Status addStatis(unsigned v1, unsigned v2, unsigned v3) = 0;
	/// get a statistics record;
	virtual Status getStatis(unsigned& v1, unsigned v2) = 0;
	/// save the statistics record to file;
	//virtual Status save(ofstream& file) = 0;
	/// load the statistics record from file;
	//virtual StatisticsBuffer* load(ifstream& file) = 0;
	virtual void flush() = 0;
protected:
	const unsigned HEADSPACE;
};

class OneConstantStatisticsBuffer : public StatisticsBuffer {
public:
	struct Triple {
		ID value1;
		unsigned count;
	};

private:
	StatisticsType type;
	MMapBuffer* buffer;
	const unsigned char* reader;
	unsigned char* writer;

	/// index for query;
	vector<unsigned> index;
	unsigned indexSize;
	unsigned nextHashValue;
	unsigned lastId;
	unsigned usedSpace;

	const unsigned ID_HASH;

	Triple* triples, *pos, *posLimit;
	bool first;
public:
	OneConstantStatisticsBuffer(const string path, StatisticsType type);
	virtual ~OneConstantStatisticsBuffer();
	Status addStatis(unsigned v1, unsigned v2, unsigned v3 = 0);
	Status getStatis(unsigned& v1, unsigned v2 = 0);
	Status save(MMapBuffer*& indexBuffer);
	static OneConstantStatisticsBuffer* load(StatisticsType type, const string path, char*& indexBuffer);
	/// get the subject or object ids from minID to maxID;
	Status getIDs(EntityIDBuffer* entBuffer, ID minID, ID maxID);
	Status getEntityIDs(EntityIDBuffer* entBuffer, ID minID, ID maxID);
	unsigned int getEntityCount();
	void flush();
private:
	/// write a id to buffer; isID indicate the id really is a ID, maybe is a count.
	void writeId(unsigned id, char*& ptr, bool isID);
	/// read a id from buffer;
	const char* readId(unsigned& id, const char* ptr, bool isID);
	/// judge the buffer is full;
	bool isPtrFull(unsigned len);
	/// get the value length in bytes;
	unsigned getLen(unsigned v);

	const unsigned char* decode(const unsigned char* begin, const unsigned char* end);
	const unsigned char* decodeID(const unsigned char* begin, const unsigned char* end);
	Status decodeAndInsertID(const unsigned char* begin, const unsigned char* end, EntityIDBuffer *entBuffer);
	bool find(unsigned value);
	bool find_last(unsigned value);
	bool findInsertPoint1(unsigned minID);
	bool findInsertPoint2(unsigned maxID);
};

class TwoConstantStatisticsBuffer : public StatisticsBuffer {
public:
	struct Triple{
		ID value1;
		ID value2;
		ID count;
	};

private:
	StatisticsType type;
	MMapBuffer* buffer;
	const unsigned char* reader;
	unsigned char* writer;

	Triple* index;

	unsigned lastId, lastPredicate;
	unsigned usedSpace;
	unsigned currentChunkNo;
	unsigned indexPos, indexSize;

	Triple triples[3 * 4096];
	Triple* pos, *posLimit;
	bool first;
public:
	TwoConstantStatisticsBuffer(const string path, StatisticsType type);
	virtual ~TwoConstantStatisticsBuffer();
	/// add a statistics record;
	Status addStatis(unsigned v1, unsigned v2, unsigned v3);
	/// get a statistics record;
	Status getStatis(unsigned& v1, unsigned v2);
	/// get the buffer position by a id, used in query
	Status getPredicatesByID(unsigned id, EntityIDBuffer* buffer, ID minID, ID maxID);
	/// save the statistics buffer;
	Status save(MMapBuffer*& indexBuffer);
	/// load the statistics buffer;
	static TwoConstantStatisticsBuffer* load(StatisticsType type, const string path, char*& indxBuffer);
	void flush();
private:
	/// get the value length in bytes;
	unsigned getLen(unsigned v);
	/// decode a chunk
	const uchar* decode(const uchar* begin, const uchar* end);
	/// decode id and predicate in a chunk
	const uchar* decodeIdAndPredicate(const uchar* begin, const uchar* end);
	///
	bool find(unsigned value1, unsigned value2);
	int findPredicate(unsigned,Triple*,Triple*);
	///
	bool find_last(unsigned value1, unsigned value2);
	bool find(unsigned,Triple* &,Triple* &);
	bool findID(unsigned value1,Triple*&,Triple*&);
	const uchar* decode(const uchar* begin, const uchar* end,Triple*,Triple*& ,Triple*&);

};
#endif /* STATISTICSBUFFER_H_ */
