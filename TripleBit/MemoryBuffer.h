#ifndef MEMORYBUFFER_H_
#define MEMORYBUFFER_H_

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

#define MEMORYBUFFER_DEBUG 1

class EntityIDBuffer;
class ColumnBuffer;
#include "TripleBit.h"
#include <iostream>

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>

#include <boost/serialization/vector.hpp>

using namespace std;

class MemoryBuffer {
	unsigned size;
	char* buffer;
	char* currentHead;
public:
	static unsigned pagesize;
public:
	friend class EntityIDBuffer;
	friend class ColumnBuffer;
	MemoryBuffer();
	MemoryBuffer(unsigned size);
	virtual ~MemoryBuffer();
	char* resize(unsigned increasedSize);
	Status resize(unsigned increasedSize, bool zero);
	char* getBuffer();
	char* getBuffer(int pos);
	size_t getSize() { return size; }
	size_t get_length() {return size; }
	char* get_address() { return buffer; }
	void memset(char value);
	void save(ofstream& ofile);
	void load(ifstream& ifile);

private:
};

/////////////////////////////////////////////////////////////////////////////////////////
//// class URIStatisticsBuffer;
////////////////////////////////////////////////////////////////////////////////////////
class URIStatisticsBuffer {
	MemoryBuffer* buffer;
	int chunkCount;
	unsigned short* p;
public:
	static int totalStatisticsPerPage;
	Status addCount(ID id);
	Status getCount(ID id, unsigned short& count);
	URIStatisticsBuffer();
	virtual ~URIStatisticsBuffer();
	void save(ofstream& ofile);
	void load(ifstream& ifile);
private:
	unsigned short*  getBuffer(ID id);

private:
};

///////////////////////////////////////////////////////////////////////////////////////
/// class StatementReificationTable
//////////////////////////////////////////////////////////////////////////////////////
class StatementReificationTable {
private:
	MemoryBuffer * buffer;
	ID* currentBuffer;			//current insert buffer;
	ID pos;						//current position can be inserted into;
public:
	StatementReificationTable();
	Status insertStatementReification(ID statement, ID column);
	Status getColumn(ID statement, ID& column);
	virtual ~StatementReificationTable();

	void save(ofstream& ofile);
	void load(ifstream& ifile);
private:
};

#endif /* MEMORYBUFFER_H_ */
