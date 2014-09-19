#ifndef PREDICATETABLE_H_
#define PREDICATETABLE_H_

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

#include "StringIDSegment.h"
#include "TripleBit.h"

class PredicateTable {
	StringIDSegment* prefix_segment;
	StringIDSegment* suffix_segment;
	LengthString prefix, suffix;
	LengthString searchLen;

	string SINGLE;
	string searchStr;

private:
	Status getPrefix(const char* URI);
public:
	PredicateTable() : SINGLE("single") { }
	PredicateTable(const string dir);
	virtual ~PredicateTable();
	Status insertTable(const char* str, ID& id);
	string getPredicateByID(ID id);
	Status getPredicateByID(string& URI, ID id);
	Status getIDByPredicate(const char* str, ID& id);

	size_t getSize() {
		return prefix_segment->getSize() + suffix_segment->getSize();
	}

	ID getPredicateCount(){
		return suffix_segment->idStroffPool->size();
	}

	size_t getPredicateNo();
	void dump();
public:
	static PredicateTable* load(const string dir);
};

#endif /* PREDICATETABLE_H_ */
