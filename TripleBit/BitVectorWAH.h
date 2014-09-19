#ifndef _BIT_VECTOR_H_
#define _BIT_VECTOR_H_

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
#include <string.h>
#include <vector>
#include <iostream>

using namespace std;

struct CompressBlock
{
	ID start;
	size_t size;
	size_t capacity;
	bitVector_ptr ptr;

	CompressBlock()
	{
		ptr = (bitVector_ptr)malloc( 7 * sizeof(word) );

		memset((void*)ptr, 0, 7 * sizeof(word));

		start = 0;
		capacity = 7;
		size = 0;
	}

	~CompressBlock()
	{
		free(ptr);
	}
};

struct BufferBlock{
	word* ptr;
	unsigned int startBit;
	unsigned int endBit;
};

class BitVectorWAH
{
public:
	BitVectorWAH();
	BitVectorWAH(const BitVectorWAH &bVec);
	virtual ~BitVectorWAH();
	//void initialize();
	//void destroy();
	void set(unsigned int pos, bool value=true);
	size_t getSize() const;
	vector<BufferBlock>* getVectorBuffer();
	bool getValue(unsigned int pos);
	void completeInsert();
	static bool decode(int &unit,bool& value);
	void print();
	static BitVectorWAH* convertVector(vector<bool>& flagVector);
	int getValueOnPos(uint& pos, int& chFlag, bool& isCompressed, unsigned int& chunkNo, bool& value);
private:
	static bool parseBit(word& temp, bool value);
	void insertIntoVector(unsigned int pos);
	bool parseBit(int &unitNo,bool& value);
	void increaseOnesCount(word& unit);
	void increaseZerosCount(unsigned int pos,int count);
	void addZeroCount(int count);
	void insertValue(unsigned char temp);
	void encode(unsigned short int units);
private:
	bitVector_ptr pBitVec;
	size_t bitVecSize;
	size_t capacity;
	//unsigned int startPos;
	unsigned int currentPos;
	size_t current_capacity;
	CompressBlock block;
	vector < BufferBlock > BufferList;
	BufferBlock Buff;
	unsigned int buffNo;
	unsigned int parsedBit;
public:
	//bit vector operations;
	static ID* XOR(BitVectorWAH* vec1, BitVectorWAH* vec2);
	static ID* XOR(BitVectorWAH* vec1, ID* vec2);
	static ID* XOR(ID* vec1, ID* vec2, size_t len);
	static ID* AND(BitVectorWAH* vec1, BitVectorWAH* vec2);
	static ID* AND(BitVectorWAH* vec1, ID* vec2);
	static ID* AND(ID* vec1, ID* vec2, size_t len);

	void save(ofstream& ofile);
	void load(ifstream& ifile);
private:
};

#endif //_BIT_VECTOR_H
