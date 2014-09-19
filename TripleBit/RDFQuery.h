#ifndef RDFQUERY_H_
#define RDFQUERY_H_

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

class SPARQLLexer;
class SPARQLParser;
class QuerySemanticAnalysis;
class PlanGenerator;
class TripleBitQuery;
class TripleBitQueryGraph;
class TripleBitRepository;
class TripleBitBuilder;

#include "TripleBit.h"
#include <string>
using namespace std;

class RDFQuery {
private:
	QuerySemanticAnalysis* semAnalysis;
	PlanGenerator* planGen;
	TripleBitQuery* bitmapQuery;
	TripleBitQueryGraph* queryGraph;
	TripleBitRepository* repo;
public:
	RDFQuery(TripleBitQuery* _bitmapQuery, TripleBitRepository* _repo);
	Status Execute(string& queryString, vector<string>& resultSet);
	void Print();
	virtual ~RDFQuery();
};

#endif /* RDFQUERY_H_ */
