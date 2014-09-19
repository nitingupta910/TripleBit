#ifndef HASH_H_
#define HASH_H_

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

inline ulonglong expand(ulonglong oldsize, ulonglong added, ID id)
{
	ulonglong newsize = (oldsize+added)*2;
	if(oldsize >= ulonglong(1<<30)) {
		newsize = oldsize + added * 2 + ulonglong(1<<30);
	}
#ifdef DEBUG
	printf("DEBUG: added %llu resize %llu to %llu by [%d]\n",added,oldsize,newsize,id);
#endif
	return newsize;
}

inline HashCodeType get_hash_code(const char * str){
	HashCodeType ret = 0;
	while(*str){
		ret = ret*31 + *str; // 31 for LUBM; 131 for Uniprot
		str++;
	}
	return ret;
}

inline HashCodeType get_hash_code(const char * str,size_t length) {
	HashCodeType ret = 0;
	while(length--){
		ret = ret*31 + *str; // 31 for LUBM; 131 for Uniprot
		str++;
	}
	return ret;
}

inline HashCodeType get_hash_code(LengthString * str){
	HashCodeType ret = 0;
	uint length = str->length;
	const char * ps = str->str;
	while(length--){
		ret = ret*31 + *ps;// 31 for LUBM; 131 for Uniprot
		ps++;
	}
	return ret;
}

inline HashCodeType next_prime_number( HashCodeType n )
{
	for(HashCodeType ret = n+1;;ret++){
		HashCodeType up = (HashCodeType)(sqrt((double)ret))+1000;
		if(up>=ret)
			up = ret-1;

		bool ok = true;
		for(HashCodeType p = 2;p<ret;p++){
			if(ret%p==0){
				ok = false;
				break;
			}
		}
		if(ok){
			return ret;
		}
	}
	return 0;
}

inline HashCodeType next_hash_capacity( HashCodeType current )
{
	HashCodeType newPrimeSed = current * 2;
	if(current >= HashCodeType(1<<25))
		newPrimeSed = current + HashCodeType(1<<25);
	HashCodeType ret = next_prime_number(newPrimeSed);
#ifdef DEBUG
	printf("DEBUG: resize hash table from %zd to %zd\n",current,ret);
#endif
	if(ret>0)
		return ret;
	else{
		exit(0);
	}
}
#endif /* HASH_H_ */
