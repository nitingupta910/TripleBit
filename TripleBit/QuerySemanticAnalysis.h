#ifndef QUERYSEMANTICANALYSIS_H_
#define QUERYSEMANTICANALYSIS_H_

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

#include "IRepository.h"

class IRepository;
class SPARQLParser;
class TripleBitQueryGraph;

///Semantic Analysis for SPARQL query. Transform the parse result into a query Graph.
class QuerySemanticAnalysis {

private:
	///Repository use for String and URI lookup.
	IRepository& repo;
public:
	QuerySemanticAnalysis(IRepository &repo);
	virtual ~QuerySemanticAnalysis();

	/// Perform the transformation
	bool transform(const SPARQLParser& input, TripleBitQueryGraph& output);
};

#endif /* QUERYSEMANTICANALYSIS_H_ */
