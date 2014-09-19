#ifndef _TRIPLEBIT_H_
#define _TRIPLEBIT_H_

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

#include <string>
#include <iostream>
#include <fstream>
#include <map>
#include <vector>
#include <stdlib.h>
#include <stdio.h>
#include <malloc.h>
#include <math.h>
#include <sys/time.h>
#include <stack>
using namespace std;

#include <boost/bind.hpp>
#include <boost/function.hpp>

#include "MessageEngine.h"

//bitmap settings
const unsigned int INIT_PAGE_COUNT = 1024;
const unsigned int INCREMENT_PAGE_COUNT = 1024;
const unsigned int CHUNK_SIZE = 16;

//uri settings
const unsigned int URI_INIT_PAGE_COUNT = 256;
const unsigned int URI_INCREMENT_PAGE_COUNT = 256;

//reification settings
const unsigned int REIFICATION_INIT_PAGE_COUNT = 2;
const unsigned int REIFICATION_INCREMENT_PAGE_COUNT = 2;

//column buffer settings
const unsigned int COLUMN_BUFFER_INIT_PAGE_COUNT = 2;
const unsigned int COLUMN_BUFFER_INCREMENT_PAGE_COUNT = 2;

//URI statistics buffer settings
const unsigned int STATISTICS_BUFFER_INIT_PAGE_COUNT = 1;
const unsigned int STATISTICS_BUFFER_INCREMENT_PAGE_COUNT = 1;

//entity buffer settings
const unsigned int ENTITY_BUFFER_INIT_PAGE_COUNT = 1;
const unsigned int ENTITY_BUFFER_INCREMENT_PAGE_COUNT = 2;

//hash index
const unsigned int HASH_RANGE = 200;
const unsigned int HASH_CAPACITY = 100000 / HASH_RANGE;
const unsigned int HASH_CAPACITY_INCREASE = 100000 / HASH_RANGE;
const unsigned int SECONDARY_HASH_RANGE = 10;
const unsigned int SECONDARY_HASH_CAPACITY = 100000 / SECONDARY_HASH_RANGE;
const unsigned int SECONDARY_HASH_CAPACITY_INCREASE = 100000 / SECONDARY_HASH_RANGE;

extern char* DATABASE_PATH;

//thread pool
const unsigned int THREAD_NUMBER = 8; //should be 2^n;
enum Status {
	OK = 1,
	NOT_FIND = -1,
	OUT_OF_MEMORY = -5,
	PTR_IS_FULL = -11,
	PTR_IS_NOT_FULL = -10,
	CHUNK_IS_FULL = -21,
	CHUNK_IS_NOT_FULL = -20,
	PREDICATE_NOT_BE_FINDED = -30,
	CHARBUFFER_IS_FULL = -40,
	CHARBUFFER_IS_NOT_FULL = -41,
	URI_NOT_FOUND = -50,
	URI_FOUND = -51,
	PREDICATE_FILE_NOT_FOUND = -60,
	PREDICATE_FILE_END = -61,
	REIFICATION_NOT_FOUND,
	FINISH_WIRITE,
	FINISH_READ,
	ERROR,
	SUBJECTID_NOT_FOUND,
	OBJECTID_NOT_FOUND,
	COLUMNNO_NOT_FOUND,
	BUFFER_NOT_FOUND,
	ENTITY_NOT_INCLUDED,
	NO_HIT,
	NOT_OPENED, 	// file was not previously opened
	END_OF_FILE, 	// read beyond end of file or no space to extend file
	LOCK_ERROR, 	// file is used by another program
	NO_MEMORY,
	URID_NOT_FOUND ,
	ALREADY_EXISTS,
	NOT_FOUND,
	CREATE_FAILURE,
	NOT_SUPPORT,
	ID_NOT_MATCH,
	ERROR_UNKOWN,
	BUFFER_MODIFIED,
	NULL_RESULT,
	TOO_MUCH
};

//join shape of patterns within a join variable.
enum JoinShape{
	STAR,
	CHAIN
};

enum EntityType
{
	PREDICATE = 1 << 0,
	SUBJECT = 1 << 1,
	OBJECT = 1 << 2,
	DEFAULT = -1
};

typedef long long int64;
typedef unsigned char word;
typedef word* word_prt;
typedef word_prt bitVector_ptr;
typedef unsigned int ID;
typedef unsigned int SOID;
typedef unsigned int PID;
typedef bool status;
typedef short COMPRESS_UNIT;
typedef unsigned int uint;
typedef unsigned char uchar;
typedef unsigned short     ushort;
typedef unsigned long long ulonglong;
typedef long long          longlong;
typedef size_t       OffsetType;
typedef size_t       HashCodeType;

extern const ID INVALID_ID;

#define BITVECTOR_INITIAL_SIZE 100
#define BITVECTOR_INCREASE_SIZE 100

#define BITMAP_INITIAL_SIZE 100
#define BITMAP_INCREASE_SIZE 30

#define BUFFER_SIZE 1024
//#define DEBUG 1
#ifndef NULL
#define NULL 0
#endif

//#define PRINT_RESULT 1
//#define TEST_TIME 1
#define WORD_SIZE (sizeof(word))

inline unsigned char Length_2_Type(unsigned char xLen, unsigned char yLen) {
	return (xLen - 1) * 4 + yLen;
}

//
inline unsigned char Type_2_Length(unsigned char type) {
	return (type - 1) / 4 + (type - 1) % 4 + 2;
}

inline void Type_2_Length(unsigned char type, unsigned char& xLen, unsigned char& yLen)
{
	xLen = (type - 1) / 4 + 1;
	yLen = (type - 1) % 4 + 1;
}

struct LengthString {
	const char * str;
	uint length;
	void dump(FILE * file) {
		for (uint i = 0; i < length; i++)
			fputc(str[i], file);
	}
	LengthString() :
		str(NULL), length(0) {
	}
	LengthString(const char * str) {
		this->str = str;
		this->length = strlen(str);
	}
	LengthString(const char * str, uint length) {
		this->str = str;
		this->length = length;
	}
	LengthString(const std::string & rhs) :
		str(rhs.c_str()), length(rhs.length()) {
	}
	bool equals(LengthString * rhs) {
		if (length != rhs->length)
			return false;
		for (uint i = 0; i < length; i++)
			if (str[i] != rhs->str[i])
				return false;
		return true;
	}
	bool equals(const char * str, uint length) {
		if (this->length != length)
			return false;
		for (uint i = 0; i < length; i++)
			if (this->str[i] != str[i])
				return false;
		return true;
	}
	bool equals(const char * str) {
		if(length != strlen(str))
			return false;
		for (uint i = 0; i < length; i++)
			if (this->str[i] != str[i])
				return false;
		return str[length] == 0;
	}
	bool copyTo(char * buff, uint bufflen) {
		if (length < bufflen) {
			for (uint i = 0; i < length; i++)
				buff[i] = str[i];
			buff[length] = 0;
			return true;
		}
		return false;
	}
};

#endif // _TRIPLEBIT_H_
