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

#include "BitVectorWAH.h"

BitVectorWAH::BitVectorWAH()
{
	//cout<<"bit vector construct"<<endl;
	pBitVec = (word*)malloc(BITVECTOR_INITIAL_SIZE);
	bitVecSize = 0;
	capacity = BITVECTOR_INITIAL_SIZE;
	current_capacity = BITVECTOR_INITIAL_SIZE;

	memset((void*)pBitVec, 0, capacity * sizeof(word));//initialize memory to zero

	parsedBit = 0;
	currentPos = 0;

	Buff.startBit = 0;
	Buff.endBit = 0;
	Buff.ptr = pBitVec;

	buffNo = 0;
	BufferList.push_back(Buff);
}

BitVectorWAH::~BitVectorWAH()
{
//	cout<<"delete bit vector"<<endl;
	unsigned int i;
	for(i = 0; i < BufferList.size();i++)
	{
		free(BufferList[i].ptr);
	}
}


void BitVectorWAH::set(unsigned int pos, bool value /*=true*/)
{
	if(value == false)//when value is false, it does not need to insert the value;
		return;

	if(parsedBit == 0){//initialize the start position;
		parsedBit = pos;
		BufferList[buffNo].startBit = pos;
		BufferList[buffNo].endBit = pos;
	}

	if( block.start == 0)
		block.start = pos;

	if((pos - block.start) >= block.capacity * sizeof(word)*8){
		insertIntoVector(pos);
	}else{
		unsigned int blockPos = pos - block.start;
		//locate to byte
		word* p = &block.ptr[blockPos / (sizeof(word)*8)];
		//locate to the position of byte
		(*p) = (*p) | (1 << blockPos % (sizeof(word)*8));
		block.size = blockPos + 1;
	}
}

size_t BitVectorWAH::getSize() const
{
	return bitVecSize;
	//return capacity;
}


vector<BufferBlock>* BitVectorWAH::getVectorBuffer()
{
	return &BufferList;
}

/**
 * @return \
 * 	when the bit unit is compressible return true, \
 * 	otherwise return false
 */
bool BitVectorWAH::parseBit(int & unitNo, bool& value)
{
	//cout<<"parseBit"<<endl;
	int64 temp = 0;
	short start = ( unitNo * 7 ) / 8;
	short end = ( ( unitNo+1 ) * 7 ) / 8;
	if(start != end)
		::memcpy(&temp,block.ptr + start,2*sizeof(word));
	else
		::memcpy(&temp,block.ptr + start,sizeof(word));

	temp = (temp >> (unitNo * 7 - start * 8) ) & 0x7F;

	if(temp ^ 0x7F){ //the bits are not all ones;
		if(temp == 0){//the bits are all zeros;
			value = false;
			return true;
		}else{
			unitNo = temp;
			return false;
		}
	}else {//the bits are all ones;
		value = true;
		return true;
	}
}

/**
 * Insert the remaining bits which are in block into bBitVec;
 */
void BitVectorWAH::completeInsert()
{
//	cout<<"insert complete"<<endl;
	unsigned short units = block.size / (sizeof(word)*8-1) + (block.size % 7? 1:0);
//	cout<<"block.size: "<<block.size<<" block.start: "<<block.start<<endl;
	encode(units);
}

void BitVectorWAH::insertValue(unsigned char temp)
{
	if(currentPos == current_capacity)
	{
		//the Buffer is full
		BufferList[buffNo].endBit -=1;

		//cout<<"bitVecSize: "<<bitVecSize<<endl;

		buffNo++;
		pBitVec = BufferList[buffNo].ptr;
		BufferList[buffNo].startBit = parsedBit;
		BufferList[buffNo].endBit = parsedBit;

		current_capacity = BITVECTOR_INCREASE_SIZE;
		currentPos = 0;
		pBitVec[currentPos]=temp;
		currentPos++;
		bitVecSize++;

	}else{
		pBitVec[currentPos] = temp;
		currentPos++;
		bitVecSize++;
	}
}

void BitVectorWAH::encode(unsigned short int units)
{
	int i = 0;
	for (i = 0; i < units; i++) {
		int temp = i;
		bool value;
		if (parseBit(temp, value) == false) {
			temp = temp | 0x80;
			insertValue(temp);
		} else {
			if (bitVecSize == 0) { //the first time insert into pBitVec;
				if (value == false) {
					temp = 0;
					temp = temp | 0x1;
				} else {
					temp = 0;
					temp = temp | 0x1;
					temp = temp | 0x40;
				}
				insertValue(temp);
			} else {
				if (value == false) {
				//	if ( currentPos == 0 )
				//		cout<<"currentPos == 0"<<endl;
					if ((pBitVec[currentPos - 1] & 0xC0) != 0) { //0 bits
						temp = 0;
						temp = temp | 0x1;
						insertValue(temp);
					} else {
						increaseZerosCount(currentPos - 1,1);
					}
				} else {
					//if( currentPos == 0 )
					//	cout<<"current==0"<<endl;
					if (currentPos > 0 && (pBitVec[currentPos - 1] & 0xC0) == 0x40) { //1 bits
						increaseOnesCount(pBitVec[currentPos - 1]);
					} else {
						temp = 0;
						temp = temp | 0x40;
						temp = temp | 0x1;
						insertValue(temp);
					}
				}
			}
		}
		BufferList[buffNo].endBit += 7;
		parsedBit +=7;
	}
//	cout<<"Encode over"<<endl;
}
/**
 * @param pos: It is used to record the last position where be set to 1;
 */
void BitVectorWAH::insertIntoVector(unsigned int pos)
{
//	cout<<"insertVector begin"<<endl;
	unsigned short units = block.size / (7) + (block.size % 7? 1:0);
	encode(units);
	
//	cout<<"asdf"<<endl;

	int ZeroCount = (pos - block.start - units * 7)/7;

//	cout<<"pos: "<<pos<<" Zero count: "<<ZeroCount<<endl;

	if ((pBitVec[currentPos - 1] & 0xC0) != 0) { //0 bits
		if(currentPos == current_capacity)
		{
			//current Buffer is full
			BufferList[buffNo].endBit -=1;

			//cout<<"bitVecSize: "<<bitVecSize<<endl;

			buffNo++;
			pBitVec = BufferList[buffNo].ptr;
			BufferList[buffNo].startBit = parsedBit;
			BufferList[buffNo].endBit = parsedBit;

			current_capacity = BITVECTOR_INCREASE_SIZE;
			currentPos = 0;
		}

		increaseZerosCount(currentPos, ZeroCount);

	} else {
		increaseZerosCount(currentPos - 1, ZeroCount);
	}
	
//	cout<<"bbb"<<endl;

	BufferList[buffNo].endBit += 7 * ZeroCount;
	parsedBit += 7*ZeroCount;

	int remain = pos - block.start - ZeroCount*7 - units* 7 + 1;
	block.start = pos - remain +1;
	block.size = remain;

//	cout<<" remain: "<<remain<<" block.start:"<<block.start<<" block.size: "<<block.size<<endl;
	memset((void*)block.ptr,0,7*sizeof(word));
	block.ptr[0] = block.ptr[0] | (1 << (pos-block.start));

	//cout<<"capacity: "<<capacity<<"bitVeczSize: "<<bitVecSize<<endl;
	if(capacity - bitVecSize <= 20){
		Buff.ptr = (word*)malloc(BITVECTOR_INCREASE_SIZE);
		memset((void*)Buff.ptr, 0, BITVECTOR_INCREASE_SIZE * sizeof(word));
		BufferList.push_back(Buff);
		capacity += BITVECTOR_INCREASE_SIZE;
	}
	
//	cout<<"insert vector over"<<endl;
}

/**
 * 
 * 
 * 0---1
 * 1---31
 * 
 */
void BitVectorWAH::increaseZerosCount(unsigned int pos, int count)
{
	if(count == 0)
		return;

	int cnt = count;

	word temp;

	if ( pos == currentPos -1)
	{
		cnt = cnt + pBitVec[pos];
		if( cnt > 0x3F) {
			temp = 0x3F;
			insertValue(temp);
			cnt = cnt - 0x3F;
		} else {
			//temp = cnt;
			pBitVec[pos] = cnt;
			return;
		}
	}

	while( cnt > 0x3F) {
		temp = 0;
		temp = 0x3F;
		insertValue(temp);
		cnt = cnt - 0x3F;
	}

	if( cnt > 0){
		temp = cnt;
		insertValue(temp);
	}
}

/*
 *
 * increment by 1 at a time
 *
 */
void BitVectorWAH::increaseOnesCount(word& unit)
{
	unsigned short count = unit & 0x3F;
	unsigned short temp = 0;

	count = count + 1;
	if(count > 0x3F){
		temp = 0;
		temp = temp | 0x40;
		temp = temp | 1;

		insertValue(temp);
	}else{
		unit = unit & 0xC0;
		unit = unit | count;
	}
}

/**
 * @return \
 * 		when does not need to decompress return false;\
 * 		otherwise return true;
 */
bool BitVectorWAH::decode(int& unit,bool& value)
{
	if(unit & 0x80){
		unit = unit & 0x7F;
		return false;
	}else{
		if(unit & 0x40){
			value = true;
			unit = unit & 0x3F;
		}else{
			value = false;
			unit = unit & 0x3F;
		}
		return true;
	}
}

/*
 * return the position of pos in the chunk
 * 
 */
int  BitVectorWAH::getValueOnPos(uint& pos, int& chFlag, bool& isCompressed, unsigned int& chunkNo, bool& value)
{
	unsigned int i = 0;
	int j;
	int temp;
	word* temp_ptr;
	unsigned int bitParsed = 0;

	if (BufferList[0].startBit > pos)
		return -1;

	for (i = 0; i < BufferList.size(); i++) {
		if (BufferList[i].startBit == pos) { //equals to pos
			temp = BufferList[i].ptr[0];
			isCompressed = decode(temp, value);

			if (isCompressed == true) {
				chunkNo = i;
				pos = temp * 7;
				return 0;
			} else {
				chunkNo = i;
				chFlag = temp;
				pos = 7;
				return 0;
			}
		} else if (BufferList[i].startBit < pos && BufferList[i].endBit >= pos) {

			temp_ptr = BufferList[i].ptr;
			pos = pos - BufferList[i].startBit + 1;
			bitParsed = 0;
			for (j = 0;; j++) {
				temp = temp_ptr[j];
				bool isCompressed = decode(temp, value);
				if (isCompressed == true) {
					bitParsed += 7 * temp;
				} else {
					bitParsed += 7;
				}

				if (bitParsed >= pos) {
					if (isCompressed) {
						chunkNo = i;
						pos = bitParsed;
						return j;
					} else {
						chFlag = temp;
						pos = bitParsed;
						chunkNo = i;
						return j;
					}
				}
			}
		}
	}

	return 0;
}
/**
 * @return
 * 		return the position value according to the pos;
 */
bool BitVectorWAH::getValue(unsigned int pos)
{
	unsigned int i = 0;
	int j;
	int temp;
	word* temp_ptr;
	bool value;
	bool iscompressed;
	unsigned int bitParsed;

	if(BufferList[0].startBit > pos )
		return false;

	for(i = 0; i < BufferList.size();i++)
	{
		if(BufferList[i].startBit == pos){ //equals to pos
			temp = BufferList[i].ptr[0];
			iscompressed = decode(temp,value);
			if(iscompressed == true){
				if(value == true)
					return true;
				else
					return false;
			}else{
				return temp & 0x1 ? true : false;
			}
		}else if(BufferList[i].startBit < pos && BufferList[i].endBit >= pos){ //greater than pos
			/*if( i == 0)
				return false;
			else{*/
				temp_ptr = BufferList[i].ptr;
				pos = pos - BufferList[i].startBit + 1;
				bitParsed = 0;
				for(j = 0; ; j++)
				{
					temp = temp_ptr[j];
					bool isCompressed = decode(temp,value);
					if(isCompressed == true){
						bitParsed += 7 * temp;
					}else{
						bitParsed += 7;
					}

					if(bitParsed >= pos){
						if(isCompressed){
							if(value == true)
								return true;
							else
								return false;
						}else{
							return temp & ( 1 << (pos - ( bitParsed  / 7 - 1 ) * 7 - 1)) ? true:false;
						}
					}
				}
			//}
		}
	}

	return false;
}

bool BitVectorWAH::parseBit(word& temp, bool value)
{
	if (temp ^ 0x7F) { 		//the bits are not all ones;
		if (temp == 0) {	//the bits are all zeros;
			value = false;
			return true;
		} else {
			//unitNo = temp;
			return false;
		}
	} else {				//the bits are all ones;
		value = true;
		return true;
	}
}

BitVectorWAH* BitVectorWAH::convertVector(vector<bool>& flagVector)
{
	size_t size = flagVector.size();
	size_t i;

	BitVectorWAH* bitVector = new BitVectorWAH;

	//cout<<"flagVector size: "<<size<<endl;

	for( i = 0; i < size; i++)
	{
		bitVector->set(i+1,flagVector[i]);
	}

	bitVector->completeInsert();

	return bitVector;
}

ID* BitVectorWAH::XOR(BitVectorWAH* vec1, BitVectorWAH* vec2)
{
	return NULL;
}

ID* BitVectorWAH::XOR(BitVectorWAH* vec1, ID* vec2)
{
	return NULL;
}

ID* BitVectorWAH::XOR(ID* vec1, ID* vec2,size_t len)
{
	size_t i;
	ID* ret = (ID*)malloc(sizeof(ID)*len);
	memset(ret,0,len*sizeof(ID));

	for(i = 0; i < len; i++)
	{
		ret[i] = vec1[i]^vec2[i];
	}

	return ret;
}

ID* BitVectorWAH::AND(BitVectorWAH* vec1, BitVectorWAH* vec2)
{
	return NULL;
}

ID* BitVectorWAH::AND(BitVectorWAH* vec1, ID* vec2)
{
	return NULL;
}

ID* BitVectorWAH::AND(ID* vec1, ID* vec2, size_t len)
{
	size_t i;
	ID* ret = (ID*)malloc(sizeof(ID)*len);
	memset(ret,0,len*sizeof(ID));

	for(i = 0; i < len; i++)
	{
		ret[i] = vec1[i]&vec2[i];
	}

	return ret;
}

void BitVectorWAH::print()
{
	cout<<"bit vector size: "<<this->bitVecSize<<endl;

	for(unsigned int i = 0; i < bitVecSize; i++)
	{
		//cout<< 1* this->BufferList[0].ptr[i]<<endl;
		printf("%x\n",this->BufferList[0].ptr[i]);
	}

}


void BitVectorWAH::save(ofstream& ofile)
{
	size_t size = BufferList.size();
	ofile << bitVecSize <<" ";
	ofile << capacity<<" ";
	ofile << currentPos<<" ";
	ofile << current_capacity<<" ";
	ofile << buffNo<<" ";
	ofile << parsedBit<<" ";
	ofile << size<<" ";

//	cout<<"size: "<<size<<endl;

	//ar<< BufferList.size();

	unsigned int i, j;

	for (i = 0; i < BufferList.size(); i++) {
		BufferBlock* blockTemp = (BufferBlock*) &BufferList[i];
		word* temp = blockTemp->ptr;
		for (j = 0; j < BITVECTOR_INITIAL_SIZE; j++) {
			ofile << temp[j];
		}
		ofile << blockTemp->startBit<<" ";
		ofile << blockTemp->endBit<<" ";
	}
}

void BitVectorWAH::load(ifstream& ifile)
{
	if( BufferList.size() != 0)
	{
		free(BufferList[0].ptr);
	}

	BufferList.clear();

	size_t size;

	ifile >> bitVecSize;
	ifile >> capacity;
	ifile >> currentPos;
	ifile >> current_capacity;
	ifile >> buffNo;
	ifile >> parsedBit;
	ifile >> size;

	ifile.get();


	size_t i, j;

	for (i = 0; i < size; i++) {
		BufferBlock blockTemp;
		blockTemp.ptr = (word*) malloc(BITVECTOR_INITIAL_SIZE);
		for (j = 0; j < BITVECTOR_INITIAL_SIZE; j++) {
			//ifile >> blockTemp.ptr[j];
			blockTemp.ptr[j] = ifile.get();
		}
		ifile >> blockTemp.startBit;
		ifile >> blockTemp.endBit;

		ifile.get();

		BufferList.push_back(blockTemp);
	}

//	cout<<"size: "<<size<<endl;

	pBitVec = BufferList[buffNo].ptr;
}
