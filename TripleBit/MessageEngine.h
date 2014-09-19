#ifndef MESSAGEENGINE_H_
#define MESSAGEENGINE_H_

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

class MessageEngine {
public:
	enum MessageType { INFO = 1, WARNING, ERROR , DEFAULT};
	MessageEngine();
	virtual ~MessageEngine();
	static void showMessage(char* msg, MessageType type = DEFAULT);
};

#endif /* MESSAGEENGINE_H_ */
