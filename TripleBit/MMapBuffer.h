#ifndef MMAPBUFFER_H_
#define MMAPBUFFER_H_

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

//#define VOLATILE   
class MMapBuffer {
	int fd;
	char volatile* mmap_addr;
	char* curretHead;
	string filename;
	size_t size;
public:
	char* resize(size_t incrementSize);
	char* getBuffer();
	char* getBuffer(int pos);
	void discard();
	Status flush();
	size_t getSize() { return size;}
	size_t get_length() { return size;}
	char * get_address() const { return (char*)mmap_addr; }

	virtual Status resize(size_t new_size,bool clear);
	virtual void   memset(char value);

	MMapBuffer(const char* filename, size_t initSize);
	virtual ~MMapBuffer();

public:
	static MMapBuffer* create(const char* filename, size_t initSize);
};

#endif /* MMAPBUFFER_H_ */
