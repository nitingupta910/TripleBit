#ifndef IREPOSITORY_H_
#define IREPOSITORY_H_

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

class IRepository {

public:
	IRepository(){}
	virtual ~IRepository(){}

	//virtual Status  open() = 0;
	///virtual Status  load() = 0;
	//virtual Status  create() = 0;
	//virtual void    close() = 0;

	//SO(id,string)transform
	virtual bool find_soid_by_string(SOID& soid, const std::string& str) = 0;
	virtual bool find_string_by_soid(std::string& str, SOID& soid) = 0;

	//P(id,string)transform
	virtual bool find_pid_by_string(PID& pid, const std::string& str) = 0;
	virtual bool find_string_by_pid(std::string& str, ID& pid) = 0;

	//create a Repository specific in the path .
	//static IRepository * create(const char * path);

	//Get some statistics information
	virtual int	get_predicate_count(PID pid) = 0;
	virtual int get_subject_count(ID subjectID) = 0;
	virtual int get_object_count(ID objectID) = 0;
	virtual int get_subject_predicate_count(ID subjectID, ID predicateID) = 0;
	virtual int get_object_predicate_count(ID objectID, ID predicateID) = 0;
	virtual int get_subject_object_count(ID subjectID, ID objectID) = 0;

	//scan the database;
	virtual Status getSubjectByObjectPredicate(ID oid, ID pod) = 0;
	virtual ID next() = 0;

	//Get the id by string;
	virtual bool lookup(const string& str, ID& id) = 0;
};

#endif /* IREPOSITORY_H_ */
