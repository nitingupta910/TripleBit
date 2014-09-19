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

#include "LineHashIndex.h"
#include "MMapBuffer.h"
#include "MemoryBuffer.h"
#include "BitmapBuffer.h"

#include <math.h>

/**
 * linear fit;
 * f(x)=kx + b;
 * used to calculate the parameter k and b;
 */
static bool calculateLineKB(vector<LineHashIndex::Point>& a, double& k, double& b, int pointNo)
{
	if (pointNo < 2)
		return false;

	double mX, mY, mXX, mXY;
	mX = mY = mXX = mXY = 0;
	int i;
	for (i = 0; i < pointNo; i++) {
		mX += a[i].x;
		mY += a[i].y;
		mXX += a[i].x * a[i].x;
		mXY += a[i].x * a[i].y;
	}

	if (mX * mX - mXX * pointNo == 0)
		return false;

	k = (mY * mX - mXY * pointNo) / (mX * mX - mXX * pointNo);
	b = (mXY * mX - mY * mXX) / (mX * mX - mXX * pointNo);
	return true;
}

LineHashIndex::LineHashIndex(ChunkManager& _chunkManager, IndexType type) : chunkManager(_chunkManager), indexType(type){
	// TODO Auto-generated constructor stub
	idTable = NULL;
	offsetTable = NULL;
	idTableEntries = NULL;
	offsetTableEntries = NULL;

	startID[0] = startID[1] = startID[2] = startID[3] = UINT_MAX;
}

LineHashIndex::~LineHashIndex() {
	// TODO Auto-generated destructor stub
	idTable = NULL;
	offsetTable = NULL;
	idTableEntries = NULL;
	offsetTableEntries = NULL;
}

/**
 * From startEntry to endEntry in idtableEntries build a line;
 * @param lineNo: the lineNo-th line to be build;
 */
bool LineHashIndex::buildLine(int startEntry, int endEntry, int lineNo)
{
	vector<Point> vpt;
	Point pt;
	int i;

	//build lower limit line;
	for (i = startEntry; i < endEntry; i++) {
		pt.x = idTableEntries[i];
		pt.y = i;
		vpt.push_back(pt);
	}

	double ktemp, btemp;
	int size = vpt.size();
	if (calculateLineKB(vpt, ktemp, btemp, size) == false)
		return false;

	double difference = btemp;//(vpt[0].y - (ktemp * vpt[0].x + btemp));
	double difference_final = difference;

	for (i = 1; i < size; i++) {
		difference = vpt[i].y - ktemp * vpt[i].x;//vpt[0].y - (ktemp * vpt[0].x + btemp);
		//cout<<"differnce: "<<difference<<endl;
		if (difference < difference_final == true)
			difference_final = difference;
	}
	btemp = difference_final;

	lowerk[lineNo] = ktemp;
	lowerb[lineNo] = btemp;
	startID[lineNo] = vpt[0].x;

	vpt.resize(0);
	//build upper limit line;
	for (i = startEntry; i < endEntry; i++) {
		pt.x = idTableEntries[i + 1];
		pt.y = i;
		vpt.push_back(pt);
	}

	size = vpt.size();
	calculateLineKB(vpt, ktemp, btemp, size);

	difference = btemp;//(vpt[0].y - (ktemp * vpt[0].x + btemp));
	difference_final = difference;

	for (i = 1; i < size; i++) {
		difference = vpt[i].y - ktemp * vpt[i].x; //vpt[0].y - (ktemp * vpt[0].x + btemp);
		if (difference > difference_final)
			difference_final = difference;
	}
	btemp = difference_final;

	upperk[lineNo] = ktemp;
	upperb[lineNo] = btemp;
	return true;
}

static ID splitID[3] = {255, 65535, 16777215};

Status LineHashIndex::buildIndex(unsigned chunkType)
{
	if (idTable == NULL) {
		idTable = new MemoryBuffer(HASH_CAPACITY);
		tableSize = idTable->getSize() / sizeof(unsigned);
		offsetTable = new MemoryBuffer(HASH_CAPACITY);
		idTableEntries = (ID*) idTable->getBuffer();
		offsetTableEntries = (ID*) offsetTable->getBuffer();
		tableSize = 0;
	}

	const uchar* begin, *limit, *reader;
	ID x, y;

	int lineNo = 0;
	int startEntry = 0, endEntry = 0;

	if (chunkType == 1) {
		reader = chunkManager.getStartPtr(1);
		begin = reader;
		if (chunkManager.getStartPtr(chunkType) == chunkManager.getEndPtr(chunkType))
			return OK;

		x = 0;
		reader = Chunk::readXId(reader, x);
		insertEntries(x, 0);

		reader = reader + (int) MemoryBuffer::pagesize;

		limit = chunkManager.getEndPtr(1);
		while (reader < limit) {
			x = 0;
			const uchar* temp = Chunk::skipBackward(reader);
			Chunk::readXId(temp, x);
			insertEntries(x, temp - begin);

			if(x > splitID[lineNo]) {
				startEntry = endEntry; endEntry = tableSize;
				if(buildLine(startEntry, endEntry, lineNo) == true) {
					lineNo++;
				}
			}
			reader = reader + (int) MemoryBuffer::pagesize;
		}

		reader = Chunk::skipBackward(limit);
		x = 0;
		Chunk::readXId(reader, x);
		insertEntries(x, reader - begin);

		startEntry = endEntry; endEntry = tableSize;
		if(buildLine(startEntry, endEntry, lineNo) == true) {
			lineNo++;
		}
	}


	if (chunkType == 2) {
		reader = chunkManager.getStartPtr(2);
		begin = reader;
		if (chunkManager.getStartPtr(chunkType) == chunkManager.getEndPtr(
				chunkType))
			return OK;

		x = 0;
		y = 0;
		Chunk::readYId(Chunk::readXId(reader, x), y);
		insertEntries(x + y, 0);

		reader = reader + (int) MemoryBuffer::pagesize;

		limit = chunkManager.getEndPtr(2);
		while (reader < limit) {
			x = 0;
			y = 0;
			const uchar* temp = Chunk::skipBackward(reader);
			Chunk::readYId(Chunk::readXId(temp, x), y);
			insertEntries(x + y, temp - begin);

			if((x + y) > splitID[lineNo]) {
				startEntry = endEntry; endEntry = tableSize;
				if(buildLine(startEntry, endEntry, lineNo) == true) {
					lineNo++;
				}
			}
			reader = reader + (int) MemoryBuffer::pagesize;
		}

		x = y = 0;
		reader = Chunk::skipBackward(limit);
		Chunk::readYId(Chunk::readXId(reader, x), y);
		insertEntries(x + y, reader - begin);

		startEntry = endEntry; endEntry = tableSize;
		if(buildLine(startEntry, endEntry, lineNo) == true) {
			lineNo++;
		}
	}
	return OK;
}

bool LineHashIndex::isBufferFull()
{
	return tableSize >= idTable->getSize() / 4;
}

void LineHashIndex::insertEntries(ID id, unsigned offset)
{
	if (isBufferFull() == true) {
		idTable->resize(HASH_CAPACITY);
		idTableEntries = (ID*) idTable->get_address();
		offsetTable->resize(HASH_CAPACITY);
		offsetTableEntries = (ID*) offsetTable->get_address();
	}
	idTableEntries[tableSize] = id;
	offsetTableEntries[tableSize] = offset;

	tableSize++;
}

int LineHashIndex::searchChunk(ID id)
{
	int lowerchunk, upperchunk;
	int lineNo;
	if(id < startID[0]) {
		return -1;
	} else if(id < startID[1]) {
		lineNo = 0;
	} else if(id < startID[2]) {
		lineNo = 1;
	} else if(id < startID[3]) {
		lineNo = 2;
	} else {
		lineNo = 3;
	}

 	lowerchunk = (int)::floor(lowerk[lineNo] * id + lowerb[lineNo]);
 	upperchunk = (int)::ceil(upperk[lineNo] * id + upperb[lineNo]);

 	if(upperchunk > (int)tableSize || upperchunk < 0) upperchunk = tableSize - 1;
 	if(lowerchunk < 0 || lowerchunk > (int)tableSize) lowerchunk = 0;

 	int low = lowerchunk;
 	int high = upperchunk;
	if(low > high){
                low = 0;
                high = tableSize-1;
        }

 	int mid;

 	if(low == high)
 		return low;
 	//find the first entry >= id;
 	while (low != high) {
		mid = low + (high - low) / 2;

		if(id > idTableEntries[mid]) {
			low = mid + 1;
		} else if((!mid) || id > idTableEntries[mid - 1]) {
			break;
		} else {
			high = mid;
		}
		/*if (id <= idTableEntries[mid + 1] && id >= idTableEntries[mid]) {
			if (mid == 0) {
				return mid;
			}

			if (mid > 0 && id > idTableEntries[mid]) {
				if (idTableEntries[mid + 1] == id)
					return mid + 1;
				else
					return mid;
			}

			while (mid >= 0 && idTableEntries[mid] == id) {
				if (mid > 0)
					mid--;
				else {
					return mid;
				}
			}
			//if (idTableEntries[mid + 1] == id) {
			//	return mid + 1;
			//} else {
			//	return mid + 1;
			//}
			return mid + 1;
		}
		if (id < idTableEntries[mid])
			high = mid - 1;
		if (id > idTableEntries[mid + 1])
			low = mid + 1;*/
	}

/* 	if(low == high) {
 		//return ;
 		if(low > 0 &&idTableEntries[low] > id) return low -1;
 		else return low;
 	} else if(idTableEntries[mid] == id) {
 		return mid;
 	} else if(mid > 0) {
 		return mid - 1;
 	} else {
 		return mid;
 	}*/
	int res = mid;

/* 	if(low == high) {
 		//return ;
 		if(low > 0 &&idTableEntries[low] > id) res  = low -1;
 		else res =  low;
 	} else if(idTableEntries[mid] == id) {
 		res = mid;
 	} else if(mid > 0) {
 		res = mid - 1;
 	} else {
 		res = mid;
 	}*/
	if (idTableEntries[res] >= id && res >0) {
		res--;
		while (idTableEntries[res] >= id && res > 0)
			res--;
	} else {
		while (res+1 < tableSize && idTableEntries[res] < id && idTableEntries[res + 1] < id)
			res++;
	}
	return res;
}

Status LineHashIndex::getOffsetByID(ID id, unsigned& offset, unsigned typeID)
{
	int offsetId = this->searchChunk(id);
	if (offsetId == -1) {
		//cout<<"id: "<<id<<endl;
		if (tableSize > 0 && id > idTableEntries[tableSize - 1]) {
			return NOT_FOUND;
		} else {
			offset = 0;
			return OK;
		}
	}

	unsigned pBegin = offsetTableEntries[offsetId];
	unsigned pEnd = offsetTableEntries[offsetId + 1];

	const uchar* beginPtr = NULL, *reader = NULL;
	int low, high, mid = 0, lastmid = 0;
	ID x, y;

	if (chunkManager.getTrpileCount(typeID) == 0)
		return NOT_FOUND;

	if (typeID == 1) {
		low = pBegin;
		high = pEnd;

		reader = chunkManager.getStartPtr(1) + low;
		beginPtr = chunkManager.getStartPtr(1);
		Chunk::readXId(reader, x);

		if (x == id) {
			offset = low;
			lastmid = low;
			//cout<<__FUNCTION__<<"x==id"<<endl;
			while (low > 0) {
				//x = 0;
				reader = Chunk::skipBackward(beginPtr + low);
				Chunk::readXId(reader, x);
				if (reader < beginPtr ||x < id) {
					offset = lastmid;
					return OK;
				}
				lastmid = reader - beginPtr;
				low = lastmid - 1;
			}
			offset = lastmid;
			return OK;
		} else if (x > id)
			return OK;

		//cout<<__FUNCTION__<<"low<=high typeID == 1"<<endl;
		while (low <= high) {
			//x = 0;
			mid = low + (high - low) / 2;
			if (lastmid == mid)
				break;
			lastmid = mid;
			reader = Chunk::skipBackward(beginPtr + mid);
			mid = reader - beginPtr;
			Chunk::readXId(reader, x);

			if (x == id) {
				lastmid = mid;
				while (mid > 0) {
					//x = 0;
					reader = Chunk::skipBackward(beginPtr + mid);
					Chunk::readXId(reader, x);
					if (reader < beginPtr ||x < id) {
						offset = lastmid;
						return OK;
					}
					lastmid = reader - beginPtr;
					mid = lastmid - 1;
				}
				offset = lastmid;
				return OK;
			} else if (x > id) {
				high = mid - 1;
			} else {
				low = mid + 1;
			}
/*			if (lastmid == mid)
                                break;
                        lastmid = mid;*/

		}
	/*	const uchar* tempoff = Chunk::skipBackward(beginPtr + mid);
		tempoff = Chunk::readYId(Chunk::readXId(reader, x), y);
		cout <<"1.one x:" << x << endl;
		Chunk::readYId(Chunk::readXId(reader, x), y);
		cout <<"1.two x:" <<x << endl;*/
		 if(mid <=0){
                        offset = 0;
                        return OK;
                }
                const uchar* tempoff = Chunk::readYId(Chunk::readXId(reader, x), y);
                if(x>id ){
                        offset = Chunk::skipBackward(reader-1) - beginPtr;
                }else if(x == id){
                        offset = reader - beginPtr;
                }else{
                        Chunk::readYId(Chunk::readXId(tempoff, x), y);
                        if(x <= id)
                                offset = tempoff- beginPtr;
                        else
                                offset = reader-beginPtr;

                }
                return OK;
	}

	if (typeID == 2) {
		low = pBegin;
		high = pEnd;

		reader = chunkManager.getStartPtr(2) + low;
		beginPtr = chunkManager.getStartPtr(2);

		reader = Chunk::readXId(reader, x);
		reader = Chunk::readYId(reader, y);
		if (x + y == id) {
			offset = low;
			lastmid = low;
			//cout<<__FUNCTION__<<"x + y == id typeID == 2"<<endl;
			while (low > 0) {
			//	cout <<"1.x+y:" << x+y << endl;
				//x = 0;
				//y = 0;
				reader = Chunk::skipBackward(beginPtr + low);
				Chunk::readYId(Chunk::readXId(reader, x), y);
				if (reader < beginPtr || x + y < id) {
					offset = lastmid;
					return OK;
				}
				lastmid = reader - beginPtr;
				low = lastmid - 1;
			}
			offset = lastmid;
			return OK;
		}

		if (x + y > id)
			return OK;
		//cout<<__FUNCTION__<<"low<=high"<<endl;
		while (low <= high) {
			//x = 0;
			mid = (low + high) / 2;
			reader = Chunk::skipBackward(beginPtr + mid);
			mid = reader - beginPtr;
			if (lastmid == mid)
				break;
			lastmid = mid;
			reader = Chunk::readXId(reader, x);
			reader = Chunk::readYId(reader, y);
			
			if (x + y == id) {
		//		cout <<"2.x+y:" << x+y << endl;
				lastmid = mid;
				while (mid > 0) {
					//x = y = 0;
					reader = Chunk::skipBackward(beginPtr + mid);
					Chunk::readYId(Chunk::readXId(reader, x), y);
					if (reader < beginPtr ||x + y < id) {
						offset = lastmid;
						return OK;
					}
					lastmid = reader - beginPtr;
					mid = lastmid - 1;
					//mid = reader - beginPtr;
				}
				offset = lastmid;
				return OK;
			} else if (x + y > id) {
				high = mid - 1;
			} else {
				low = mid +1;
			}
	/*		if (lastmid == mid)
                                break;
                        lastmid = mid;*/

		}
/*		cout<<"low:"<<low <<"  mid:"<<mid <<"  high:" << high << endl;
		const uchar* tempoff = Chunk::skipBackward(beginPtr + mid);
                tempoff = Chunk::readYId(Chunk::readXId(reader, x), y);
                cout <<"2.one x+y:" << x+y <<"  offset:"<<(tempoff-beginPtr) << endl;
                Chunk::readYId(Chunk::readXId(tempoff, x), y);
                cout <<"2.two x+y:" <<x+y << endl;
		if(x+y ==id){
		   cout<<"lalalala" << endl;
		}	 */
		if(mid <=0){
			offset = 0;
			return OK;
		}
		const uchar* tempoff = Chunk::readYId(Chunk::readXId(reader, x), y);
		if(x+y >id ){
			offset = Chunk::skipBackward(reader-1) - beginPtr;
		}else if(x+y == id){
			offset = reader - beginPtr;
		}else{
			Chunk::readYId(Chunk::readXId(tempoff, x), y);
			if(x+y <= id)
				offset = tempoff- beginPtr;
			else
				offset = reader-beginPtr;
			
		}
		return OK;

	}
/*	if (mid <= 0)i
		offset = 0;
	else{
		offset = Chunk::skipBackward(beginPtr + mid) - beginPtr;
		
	}
	return OK;*/
}

void LineHashIndex::save(MMapBuffer*& indexBuffer)
{
	char* writeBuf;

	if (indexBuffer == NULL) {
		indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/BitmapBuffer_index").c_str(), tableSize * 4 * 2 + 4 + 16 * sizeof(double) + 4 * sizeof(ID));
		writeBuf = indexBuffer->get_address();
	} else {
		size_t size = indexBuffer->getSize();
		writeBuf = indexBuffer->resize(tableSize * 4 * 2 + 4 + 16 * sizeof(double) + 4 * sizeof(ID)) + size;
	}

	*(ID*) writeBuf = tableSize;
	writeBuf = writeBuf + 4;
	memcpy(writeBuf, (char*) idTableEntries, tableSize * 4);
	writeBuf = writeBuf + tableSize * 4;
	memcpy(writeBuf, (char*) offsetTableEntries, tableSize * 4);
	writeBuf = writeBuf + tableSize * 4;

	for(int i = 0; i < 4; i++) {
		*(ID*)writeBuf = startID[i];
		writeBuf = writeBuf + sizeof(ID);

		*(double*)writeBuf = lowerk[i];
		writeBuf = writeBuf + sizeof(double);
		*(double*)writeBuf = lowerb[i];
		writeBuf = writeBuf + sizeof(double);

		*(double*)writeBuf = upperk[i];
		writeBuf = writeBuf + sizeof(double);
		*(double*)writeBuf = upperb[i];
		writeBuf = writeBuf + sizeof(double);
	}

	indexBuffer->flush();
	delete idTable;
	idTable = NULL;
	delete offsetTable;
	offsetTable = NULL;
}

LineHashIndex* LineHashIndex::load(ChunkManager& manager, IndexType type, char* buffer, size_t& offset)
{
	LineHashIndex* index = new LineHashIndex(manager, type);
	char* base = buffer + offset;
	index->tableSize = *((ID*)base);
	index->idTableEntries = (ID*)(base + 4);
	index->offsetTableEntries = (ID*)(index->idTableEntries + index->tableSize);
	offset = offset + 4 + 4 * 2 * index->tableSize;

	base = buffer + offset;
	for(int i = 0; i < 4; i++) {
		//base = buffer + offset;
		index->startID[i] = *(ID*)base;
		base = base + sizeof(ID);

		index->lowerk[i] = *(double*)base;
		base = base + sizeof(double);
		index->lowerb[i] = *(double*)base;
		base = base + sizeof(double);

		index->upperk[i] = *(double*)base;
		base = base + sizeof(double);
		index->upperb[i] = *(double*)base;
		base = base + sizeof(double);
	}

	offset = offset + 16 * sizeof(double) + 4 * sizeof(ID);
	return index;
}
