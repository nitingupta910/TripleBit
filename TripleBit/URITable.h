#ifndef URITABLE_H_
#define URITABLE_H_

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

#include "TripleBit.h"
#include "StringIDSegment.h"

using namespace std;
class URITable {
	ID prefixStartID;
	StringIDSegment* prefix_segment;
	StringIDSegment* suffix_segment;
	LengthString prefix, suffix;
	LengthString searchLen;

	string SINGLE;
	string searchStr;

private:
	Status getPrefix(const char*  URI);
public:
	URITable();
	URITable(const string dir);
	virtual ~URITable();
	Status insertTable(const char* URI,ID& id);
	Status getIdByURI(const char* URI,ID& id);
	Status getURIById(string& URI,ID id);

	size_t getSize() {
		cout<<"max id: "<<suffix_segment->getMaxID()<<endl;
		return prefix_segment->getSize() + suffix_segment->getSize();
	}

	ID getUriCount(){
		return suffix_segment->idStroffPool->size();
	}

	void dump();
public:
	static ID startID;
	static URITable* load(const string dir);
	static ID getMaxID();
};

#endif /* URITABLE_H_ */
