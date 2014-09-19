#ifndef TRIPLEBITRESPOSITORY_H_
#define TRIPLEBITRESPOSITORY_H_

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

class TripleBitBuilder;
class PredicateTable;
class URITable;
class URIStatisticsBuffer;
class BitmapBuffer;
class FindEntityID;
class EntityIDBuffer;
class MMapBuffer;
class TripleBitQuery;
class RDFQuery;

#include "IRepository.h"
#include "TripleBit.h"
#include "StatisticsBuffer.h"

class TripleBitRepository : public IRepository{
	PredicateTable * preTable;
	URITable* UriTable;
	BitmapBuffer* bitmapBuffer;
	StatisticsBuffer* subjectStat, *subPredicateStat, *objectStat, *objPredicateStat;
	FindEntityID* columnFinder;
	EntityIDBuffer* buffer;
	int pos;

	MMapBuffer* bitmapImage, *bitmapIndexImage, *bitmapPredicateImage;

	vector<string> resultSet;
	vector<string>::iterator resBegin;
	vector<string>::iterator resEnd;

	TripleBitQuery* bitmapQuery;
	RDFQuery* query;
public:
	TripleBitRepository();
	virtual ~TripleBitRepository();

	//SO(id,string)transform
	bool find_soid_by_string(SOID& soid, const std::string& str);
	bool find_string_by_soid(std::string& str, SOID& soid);

	//P(id,string)transform
	bool find_pid_by_string(PID& pid, const std::string& str);
	bool find_string_by_pid(std::string& str, ID& pid);

	//create a Repository specific in the path .
	static TripleBitRepository * create(const string path);

	//Get some statistics information
	int	get_predicate_count(PID pid);
	int get_subject_count(ID subjectID);
	int get_object_count(ID objectID);
	int get_subject_predicate_count(ID subjectID, ID predicateID);
	int get_object_predicate_count(ID objectID, ID predicateID);
	int get_subject_object_count(ID subjectID, ID objectID);

	PredicateTable* getPredicateTable() const { return preTable; }
	URITable* getURITable() const { return UriTable; }
	BitmapBuffer* getBitmapBuffer() const { return bitmapBuffer; }

	StatisticsBuffer* getStatisticsBuffer(StatisticsBuffer::StatisticsType type) {
		switch(type) {
		case StatisticsBuffer::SUBJECT_STATIS:
			return subjectStat;
		case StatisticsBuffer::OBJECT_STATIS:
			return objectStat;
		case StatisticsBuffer::SUBJECTPREDICATE_STATIS:
			return subPredicateStat;
		case StatisticsBuffer::OBJECTPREDICATE_STATIS:
			return objPredicateStat;
		}

		return NULL;
	}
	//scan the database;
	Status getSubjectByObjectPredicate(ID oid, ID pod);
	ID next();

	//lookup string id;
	bool lookup(const string& str, ID& id);
	Status nextResult(string& str);
	Status execute(string query);
	size_t getResultSize() const { return resultSet.size(); }

	void cmd_line(FILE* fin, FILE* fout);
	static int colNo;
};

#endif /* TRIPLEBITRESPOSITORY_H_ */
