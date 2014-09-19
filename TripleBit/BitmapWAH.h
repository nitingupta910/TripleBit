#ifndef _BITMAP_H_
#define _BITMAP_H_

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
#include "BitVectorWAH.h"
#include <map>

using namespace std;

class BitmapWAH
{

public:
	BitmapWAH();
	void insert(ID id, unsigned int pos);
	void print();
	size_t get_size();
	void completeInsert();
	//BitVector* getBitVector(ID id);
	virtual ~BitmapWAH();
private:
	bool isIdInBitmap(ID id);
	void expandBitmap();
	//BitVector* getBitVector(ID id);
private:
	typedef BitVectorWAH* BitMapType;
	map<ID,BitMapType> bitMap;
	size_t bitMapSize;
	unsigned int capacity;
};

#endif // !defined _BITMAP_H_
