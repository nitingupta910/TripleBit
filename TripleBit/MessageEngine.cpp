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

#include "MessageEngine.h"
#include "TripleBit.h"

MessageEngine::MessageEngine() {
	// TODO Auto-generated constructor stub

}

MessageEngine::~MessageEngine() {
	// TODO Auto-generated destructor stub
}

void MessageEngine::showMessage(char* msg, MessageType type /* = DEFAULT*/)
{
	switch(type)
	{
	case INFO:
		fprintf(stderr, "\033[0;32mINFO: %s\033[0m.\n",msg);
		break;
	case WARNING:
		fprintf(stderr, "\033[1;33mWARNING: %s\033[0m.\n",msg);
		break;
	case ERROR:
		fprintf(stderr, "\033[0;31mERROR: %s\033[0m.\n",msg);
		break;
	default:
		printf("%s\n", msg);
	}
}
