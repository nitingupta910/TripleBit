#ifndef STRINGIDSEGMENT_H_
#define STRINGIDSEGMENT_H_

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
#include "ObjectPool.h"
#include "MMapBuffer.h"

//idStroffPool entry
struct IDStroffEntry{
	OffsetType stroff;
};

class StringIDSegment {
	double                     fillRate;  // max hash table fill rate
public:
	ObjectPool<uint>         * stringPool; // a var strings table(length,string;length,string;...)
	MMapBuffer             * stringHashTable;  // hashtable for fast string to str_off in stringPool finding
	FixedObjectPool          * idStroffPool; // a fix pool(id,stroff) for id to string

public:
	StringIDSegment();

	virtual ~StringIDSegment();

private:
	//
	void buildStringHashTable();

	//add string to stringPool,and update the stringHashTable.
	OffsetType addStringToStringPoolAndUpdateStringHashTable( LengthString * aStr, ID id);

	ID addIDStroffToIdStroffPool(IDStroffEntry * entry){
		return idStroffPool->append_object_get_id(entry);
	}

	OffsetType findStringOffset(LengthString * aStr);
public:
	//add string to StringIDSegment ,update stringPool,stringHashTable,idStroffPool.
	ID addStringToSegment(LengthString * aStr);

	ID addStringToSegment(std::string& aStr){
		LengthString* lstr = new LengthString(aStr);
		ID id = addStringToSegment(lstr);
		delete lstr;
		return id;
	}

	ID addStringToSegment(const char* str) {
		LengthString* lstr = new LengthString(str);
		ID id = addStringToSegment(lstr);
		delete lstr;
		return id;
	}
	//reverse more memory for fast insert.
	void reserveStringPoolSpace(OffsetType size){
		stringPool->reserve(size);
	}
	//
	void reserveIdStroffPoolSpace(OffsetType size){
		idStroffPool->reserve(size);
	}

	//Optimize memory
	Status optimize();



	//(id ,string) finding
	bool findStringById(std::string& aStr, const ID& id);
	bool findIdByString(ID& id, const std::string& aStr);

	//second way (id,string) finding
	bool findStringById(LengthString * aStr, const ID& id);
	bool findIdByString(ID& id, LengthString * aStr);

	bool findIdByString(ID& id, const char* str) {
		LengthString* lstr = new LengthString(str);
		bool b = findIdByString(id, lstr);
		delete lstr;
		return b;
	}

	size_t getSize() {
		cout<<stringPool->usage()<<" : "<<stringHashTable->get_length()<<" : "<<idStroffPool->usage()<<endl;
		return stringPool->usage() + stringHashTable->get_length() + idStroffPool->usage();
	}
	//for test
	void cmd_line(FILE * fin,FILE * fout);
	void dump();

	ID getMaxID();
public:
	static StringIDSegment* create(const string dir, const string segmentName);
	static StringIDSegment* load(const string dir, const string segmentName);
};

#endif /* STRINGIDSEGMENT_H_ */
