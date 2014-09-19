#ifndef TRIPLEBITBUILDER_H_
#define TRIPLEBITBUILDER_H_

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

#define TRIPLEBITBUILDER_DEBUG 1

class PredicateTable;
class URITable;
class URIStatisticsBuffer;
class StatementReificationTable;
class FindColumns;
class BitmapBuffer;
class Sorter;
class TempFile;
class StatisticsBuffer;

#include "TripleBit.h"
#include "StatisticsBuffer.h"

#include <fstream>
#include <pthread.h>
#include <cassert>
#include <cstring>

#include "TurtleParser.h"
#include "ThreadPool.h"
#include "TempFile.h"

using namespace std;

class TripleBitBuilder {
private:
	//MySQL* mysql;
	BitmapBuffer* bitmap;
	PredicateTable* preTable;
	URITable* uriTable;
	vector<string> predicates;
	string dir;
	/// statistics buffer;
	StatisticsBuffer* statBuffer[4];
	StatementReificationTable* staReifTable;
	FindColumns* columnFinder;
	fstream statementFile;
public:
	TripleBitBuilder();
	TripleBitBuilder(const string dir);
	Status initBuild();
	Status startBuild();
	static const char* skipIdIdId(const char* reader);
	static int compareValue(const char* left, const char* right);
	static int compare213(const char* left, const char* right);
	static int compare231(const char* left, const char* right);
	static int compare123(const char* left, const char* right);
	static int compare321(const char* left, const char* right);
	static inline void loadTriple(const char* data, ID& v1, ID& v2, ID& v3) {
		TempFile::readId(TempFile::readId(TempFile::readId(data, v1), v2), v3);
	}

	static inline int cmpValue(ID l ,ID r) {
		return (l < r) ? -1 : ((l > r) ? 1 : 0);
	}
	static inline int cmpTriples(ID l1, ID l2, ID l3, ID r1, ID r2, ID r3) {
		int c = cmpValue(l1, r1);
		if(c)
			return c;
		c = cmpValue(l2, r2);
		if(c)
			return c;
		return cmpValue(l3, r3);

	}
	StatisticsBuffer* getStatBuffer(StatisticsBuffer::StatisticsType type) {
		return statBuffer[static_cast<int>(type)];
	}

	Status resolveTriples(TempFile& rawFacts, TempFile& facts);
	Status startBuildN3(string fileName);
	bool N3Parse(istream& in, const char* name, TempFile&);
	Status importFromMySQL(string db, string server, string username, string password);
	void NTriplesParse(const char* subject, const char* predicate, const char* object, TempFile&);
	ID generateXY(ID& subjectID, ID& objectID);
	Status buildIndex();
	Status endBuild();
	
	static bool isStatementReification(const char* object);
	virtual ~TripleBitBuilder();
};
#endif /* TRIPLEBITBUILDER_H_ */
