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

#include "BitmapWAH.h"

#if _MSC_VER > 1000
#pragma once
#pragma warning(disable: 4786)
#endif // _MSC_VER > 1000
//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

BitmapWAH::BitmapWAH()
{
//	bitMap = (BitMapType*)malloc(sizeof(BitMapType) * BITMAP_INITIAL_SIZE);
	/*bitMap = Alloc::allocate(BITMAP_INITIAL_SIZE);
	bitMapSize = 0;
	capacity = BITMAP_INITIAL_SIZE;
	unsigned int i;
	for(i = 0; i < capacity; i++)
	{
		bitMap[i] = NULL;
	}*/
	bitMapSize = 0;
}

BitmapWAH::~BitmapWAH()
{
	//unsigned int size;
	/*unsigned int i;
	for( i = 0; i < bitMapSize; i++)
	{
		//bitMap[i]->destroy();
		///free(bitMap[i]);
		delete bitMap[i];
	}

	free(bitMap);*/

	cout<<"destroy bitmap"<<endl;
	map<ID,BitMapType>::iterator iter = bitMap.begin();
	for(;iter != bitMap.end();iter++)
	{
		delete iter->second;
	}
}

/************************************************************************/
/*                                                                      */
/************************************************************************/
void BitmapWAH::insert(ID id, unsigned int pos)
{
	/*if(id < 0)
		return;
	if(id <= capacity){
		if(bitMap[id-1] == NULL){
			BitVector * pBitVec = new BitVector;
			bitMap[id-1] = pBitVec;
			bitMapSize = id;
		}
		bitMap[id-1]->set(pos);
		cout<<bitMapSize<<endl;
	}else{
		expandBitmap();
		BitVector * pBitVec = (BitVector*)malloc(sizeof(BitVector));
		pBitVec->initialize();
		BitVector * pBitVec = new BitVector;
		bitMap[id-1] = pBitVec;
		bitMap[id-1]->set(pos);
		bitMapSize = id;

		cout<<bitMapSize<<endl;
	}*/
	if(isIdInBitmap(id) == true)
	{
		bitMap[id]->set(pos);
	}else{
		BitVectorWAH* pBitVec = new BitVectorWAH;
		bitMap[id] = pBitVec;
		bitMap[id]->set(pos);
		bitMapSize++;
	}
}

void BitmapWAH::expandBitmap()
{
	/*bitMap = (BitMapType*)realloc(bitMap, BITMAP_INCREASE_SIZE * sizeof(BitVector*));
	int i = 0;
	//for(i = 0; i<BITMAP_INCREASE_SIZE;i++)
	//{
		//bitMap[capacity + i] = NULL;
		memset(bitMap+capacity,0,sizeof(BitVector*)*BITMAP_INCREASE_SIZE);
	//}
	capacity += BITMAP_INCREASE_SIZE;*/
}

bool BitmapWAH::isIdInBitmap(ID id)
{
	/*if(id > 0 && id <= bitMapSize)
		return true;
	else
		return false;*/

	map<ID,BitMapType>::iterator iter = bitMap.find(id);
	if(iter == bitMap.end())
		return false;
	else
		return true;
}

size_t BitmapWAH::get_size()
{
	size_t size = 0;

	map<ID,BitMapType>::iterator iter = bitMap.begin();

	for(;iter != bitMap.end();iter++)
	{
		size+=(iter->second)->getSize();
	}
	return size;
}


/*********************************************************************/
/*                                                                      */
/************************************************************************/
void BitmapWAH::print()
{
	cout<<"the bit map size is "<<get_size()<<endl;

}

void BitmapWAH::completeInsert()
{
	/*int i;
	for(i = 0; i< bitMapSize; i++)
	{
		if(bitMap[i] != NULL)
			bitMap[i]->completeInsert();
	}*/

	map<ID,BitMapType>::iterator iter = bitMap.begin();

	for(;iter != bitMap.end(); iter++)
	{
		(iter->second)->completeInsert();
	}
}
