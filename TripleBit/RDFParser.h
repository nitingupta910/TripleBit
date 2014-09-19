#ifndef RDFPARSER_H_
#define RDFPARSER_H_

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
#include <raptor.h>
#include <string>
#include <iostream>

using namespace std;

class RDFParser {
public:
	RDFParser();
	static void parserRDFFile(string fileName, raptor_statement_handler hanler, void * user_data);
	//static void parseRdf(void* user_data, const raptor_statement* triple);
	virtual ~RDFParser();
private:
	//std::string fileName;

};

#endif /* RDFPARSER_H_ */
