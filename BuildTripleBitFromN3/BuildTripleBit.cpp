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

#include "TripleBitBuilder.h"
#include "OSFile.h"
#include <raptor.h>

char* DATABASE_PATH;
int main(int argc, char* argv[])
{
	if(argc != 3) {
		fprintf(stderr, "Usage: %s <N3 file name> <Database Directory>\n", argv[0]);
		return -1;
	}

	if(OSFile::directoryExists(argv[2]) == false) {
		OSFile::mkdir(argv[2]);
	}

	DATABASE_PATH = argv[2];
	TripleBitBuilder* builder = new TripleBitBuilder(argv[2]);
	builder->startBuildN3(argv[1]);
	builder->endBuild();
 	delete builder;

 	return 0;
}
