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

#include "MemoryBuffer.h"
#include "BitmapBuffer.h"
#include "BitVectorWAH.h"
#include "MMapBuffer.h"
#include "TempFile.h"

unsigned int ChunkManager::bufferCount = 0;

//#define WORD_ALIGN 1

BitmapBuffer::BitmapBuffer(const string _dir) : dir(_dir) {
	// TODO Auto-generated constructor stub
	startColID = 1;
	string filename(dir);
	filename.append("/temp1");
	temp1 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);

	filename.assign(dir.begin(), dir.end());
	filename.append("/temp2");
	temp2 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);

	filename.assign(dir.begin(), dir.end());
	filename.append("/temp3");
	temp3 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT* MemoryBuffer::pagesize);

	filename.assign(dir.begin(), dir.end());
	filename.append("/temp4");
	temp4 = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);

	usedPage1 = usedPage2 = usedPage3 = usedPage4 = 0;
}

BitmapBuffer::~BitmapBuffer() {
	// TODO Auto-generated destructor stub
	for(map<ID, ChunkManager*>::iterator iter = predicate_managers[0].begin(); iter != predicate_managers[0].end(); iter++) {
		if(iter->second != 0) {
			delete iter->second;
			iter->second = NULL;
		}
	}

	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[1].begin(); iter != predicate_managers[1].end(); iter++) {
		if (iter->second != 0) {
			delete iter->second;
			iter->second = NULL;
		}
	}
}

Status BitmapBuffer::insertPredicate(ID id,unsigned char type) {
	predicate_managers[type][id] = new ChunkManager(id, type, this);
	return OK;
}

Status BitmapBuffer::deletePredicate(ID id) {
	//TODO
	return OK;

}

size_t BitmapBuffer::getTripleCount()
{
	size_t tripleCount = 0;
	map<ID, ChunkManager*>::iterator begin, limit;
	for(begin = predicate_managers[0].begin(), limit = predicate_managers[0].end(); begin != limit; begin++) {
		tripleCount = tripleCount + begin->second->getTripleCount();
	}
	cout<<"triple count: "<<tripleCount<<endl;

	tripleCount = 0;
	for(begin = predicate_managers[1].begin(), limit = predicate_managers[1].end(); begin != limit; begin++) {
		tripleCount = tripleCount + begin->second->getTripleCount();
	}
	cout<<"triple count: "<<tripleCount<<endl;

	return tripleCount;
}

/*
 *	@param id: the chunk manager id ( predicate id );
 *       type: the predicate_manager type;
 */
ChunkManager* BitmapBuffer::getChunkManager(ID id, unsigned char type) {
	//there is no predicate_managers[id]
	if(!predicate_managers[type].count(id)) {
		//the first time to insert
		insertPredicate(id, type);
	}
	return predicate_managers[type][id];
}


/*
 *	@param f: 0 for triple being sorted by subject; 1 for triple being sorted by object
 *         flag: indicate whether x is bigger than y;
 */
Status BitmapBuffer::insertTriple(ID predicateId, ID xId, ID yId, bool flag, unsigned char f) {
	unsigned char len;

	len = getLen(xId);
	len += getLen(yId);

	if ( flag == false){
		getChunkManager(predicateId, f)->insertXY(xId, yId, len, 1);
	}else {
		getChunkManager(predicateId, f)->insertXY(xId, yId, len, 2);
	}

//	cout<<getChunkManager(1, 0)->meta->length[0]<<" "<<getChunkManager(1, 0)->meta->tripleCount[0]<<endl;
	return OK;
}

Status BitmapBuffer::completeInsert()
{
	//TODO do something after complete;
	startColID = 1;

	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[0].begin(); iter != predicate_managers[0].end(); iter++ ){
		if(iter->second != 0) {
			iter->second->setColStartAndEnd(startColID);
		}
	//	cout<<startColID<<endl;
	}

	startColID = 1;
	for (map<ID, ChunkManager*>::iterator iter = predicate_managers[1].begin(); iter != predicate_managers[1].end(); iter++ ){
		if(iter->second != 0) {
			iter->second->setColStartAndEnd(startColID);
		}
	//	cout<<startColID<<endl;
	}

	return OK;
}

void BitmapBuffer::insert(ID predicateID,ID subjectID,ID objectID)
{
	bool isBigger = false; //sequence flag of subject and object
	if(subjectID > objectID){
		isBigger = true;
	}

	generateXY(subjectID,objectID);

	unsigned char xLen,yLen;
	xLen = getBytes(subjectID);
	//yLen = GetBytes(objectID);
	yLen = 4;

	//	cout<<__FUNCTION__<<endl;
//	insertTriple(predicateID,subjectID,xLen,objectID,yLen,isBigger);
}

void BitmapBuffer::flush()
{
	temp1->flush();
	temp2->flush();
	temp3->flush();
	temp4->flush();
}

void BitmapBuffer::generateXY(ID& subjectID, ID& objectID)
{
	ID temp;

	if(subjectID > objectID)
	{
		temp = subjectID;
		subjectID = objectID;
		objectID = temp - objectID;
	}else{
		objectID = objectID - subjectID;
	}
}

unsigned char BitmapBuffer::getBytes(ID id)
{
	if(id <=0xFF){
		return 1;
	}else if(id <= 0xFFFF){
		return 2;
	}else if(id <= 0xFFFFFF){
		return 3;
	}else if(id <= 0xFFFFFFFF){
		return 4;
	}else{
		return 0;
	}
}

char* BitmapBuffer::getPage(unsigned char type, unsigned char flag, size_t& pageNo)
{
	char* rt;
	bool tempresize = false;
	MMapBuffer *temp = NULL;

	//cout<<__FUNCTION__<<" begin"<<endl;

	if(type == 0 ) {
		if(flag == 0) {
			if(usedPage1 * MemoryBuffer::pagesize >= temp1->getSize()) {
				temp1->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
				temp = temp1;
				tempresize = true;
			}
			pageNo = usedPage1;
			rt = temp1->get_address() + usedPage1 * MemoryBuffer::pagesize;
			usedPage1++;
		} else {
			if(usedPage2 * MemoryBuffer::pagesize >= temp2->getSize()) {
				temp2->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
				temp = temp2;
				tempresize = true;
			}
			pageNo = usedPage2;
			rt = temp2->get_address() + usedPage2 * MemoryBuffer::pagesize;
			usedPage2++;
		}
	} else {
		if(flag == 0) {
			if(usedPage3 * MemoryBuffer::pagesize >= temp3->getSize()) {
				temp3->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
				temp = temp3;
				tempresize = true;
			}
			pageNo = usedPage3;
			rt = temp3->get_address() + usedPage3 * MemoryBuffer::pagesize;
			usedPage3++;
		} else {
			if(usedPage4 * MemoryBuffer::pagesize >= temp4->getSize()) {
				temp4->resize(INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
				temp = temp4;
				tempresize = true;
			}
			pageNo = usedPage4;
			rt = temp4->get_address() + usedPage4 * MemoryBuffer::pagesize;
			usedPage4++;
		}
	}

	if(tempresize == true ) {
		if(type == 0) {
			if(flag == 0) {
				map<ID, ChunkManager*>::iterator iter, limit;
				iter = predicate_managers[0].begin(); limit = predicate_managers[0].end();
				for(; iter != limit; iter++) {
					if(iter->second == NULL)
						continue;
					iter->second->meta = (ChunkManagerMeta*)(temp1->get_address() + iter->second->usedPage[0][0] * MemoryBuffer::pagesize);
					if(iter->second->usedPage[0].size() == 1) {
						iter->second->meta->endPtr[0] = temp1->get_address() + iter->second->usedPage[0].back() * MemoryBuffer::pagesize +
								MemoryBuffer::pagesize - (iter->second->meta->length[0] - iter->second->meta->usedSpace[0] - sizeof(ChunkManagerMeta));
					} else {
						iter->second->meta->endPtr[0] = temp1->get_address() + iter->second->usedPage[0].back() * MemoryBuffer::pagesize +
								MemoryBuffer::pagesize - (iter->second->meta->length[0] - iter->second->meta->usedSpace[0] - sizeof(ChunkManagerMeta));
					}
					iter->second->meta->endPtr[1] = temp2->get_address() + iter->second->usedPage[1].back() * MemoryBuffer::pagesize +
							MemoryBuffer::pagesize - (iter->second->meta->length[1] - iter->second->meta->usedSpace[1]);
				}
			} else {
				map<ID, ChunkManager*>::iterator iter, limit;
				iter = predicate_managers[0].begin(); limit = predicate_managers[0].end();
				for(; iter != limit; iter++) {
					if(iter->second == NULL)
						continue;
					iter->second->meta->endPtr[1] = temp2->get_address() + iter->second->usedPage[1].back() * MemoryBuffer::pagesize +
							MemoryBuffer::pagesize - (iter->second->meta->length[1] - iter->second->meta->usedSpace[1]);
				}
			}
		} else if(type == 1) {
			if(flag == 0) {
				map<ID, ChunkManager*>::iterator iter, limit;
				iter = predicate_managers[1].begin(); limit = predicate_managers[1].end();
				for(; iter != limit; iter++) {
					if(iter->second == NULL)
						continue;
					iter->second->meta = (ChunkManagerMeta*)(temp3->get_address() + iter->second->usedPage[0][0] * MemoryBuffer::pagesize);
					if(iter->second->usedPage[0].size() == 1) {
						iter->second->meta->endPtr[0] = temp3->get_address() + iter->second->usedPage[0].back() * MemoryBuffer::pagesize +
								MemoryBuffer::pagesize - (iter->second->meta->length[0] - iter->second->meta->usedSpace[0] - sizeof(ChunkManagerMeta));
					} else {
						iter->second->meta->endPtr[0] = temp3->get_address() + iter->second->usedPage[0].back() * MemoryBuffer::pagesize +
								MemoryBuffer::pagesize - (iter->second->meta->length[0] - iter->second->meta->usedSpace[0] - sizeof(ChunkManagerMeta));
					}
					iter->second->meta->endPtr[1] = temp4->get_address() + iter->second->usedPage[1].back() * MemoryBuffer::pagesize +
							MemoryBuffer::pagesize - (iter->second->meta->length[1] - iter->second->meta->usedSpace[1]);
				}
			} else {
				map<ID, ChunkManager*>::iterator iter, limit;
				iter = predicate_managers[1].begin(); limit = predicate_managers[1].end();
				for(; iter != limit; iter++) {
					if(iter->second == NULL)
						continue;
					iter->second->meta->endPtr[1] = temp4->get_address() + iter->second->usedPage[1].back() * MemoryBuffer::pagesize +
							MemoryBuffer::pagesize - (iter->second->meta->length[1] - iter->second->meta->usedSpace[1]);
				}
			}
		}
	}

	//cout<<__FUNCTION__<<" end"<<endl;

	return rt;
}

unsigned char BitmapBuffer::getLen(ID id) {
	unsigned char len = 0;
	while(id >= 128) {
		len++;
		id >>= 7;
	}
	return len + 1;
}

void BitmapBuffer::save()
{
	//loadfromtemp();
	static bool first = true;
	string filename = dir + "/BitmapBuffer";
	MMapBuffer * buffer; //new MMapBuffer(filename.c_str(), 0);
	string predicateFile(filename);
	predicateFile.append("_predicate");

	MMapBuffer * predicateBuffer =new MMapBuffer(predicateFile.c_str(), predicate_managers[0].size() * (sizeof(ID) + sizeof(size_t)) * 2);

	char* predicateWriter = predicateBuffer->get_address();
	char* bufferWriter = NULL;

	map<ID, ChunkManager*>::const_iterator iter = predicate_managers[0].begin();
	char* startPtr;
	size_t offset = 0;
	startPtr = iter->second->ptrs[0];

	if(first == true) {
		buffer = new MMapBuffer(filename.c_str(), iter->second->meta->length[0]);
		//offset = buffer->get_offset();
		//first = false;
	} else {
		//buffer = new MMapBuffer(filename.c_str(), iter->second->meta->length[0], true);
		//offset = buffer->get_offset();
	}

	predicateWriter = predicateBuffer->get_address();
	bufferWriter = buffer->get_address();
	vector<size_t>::iterator pageNoIter = iter->second->usedPage[0].begin(),
			limit = iter->second->usedPage[0].end();

	for(; pageNoIter != limit; pageNoIter++ ) {
		size_t pageNo = *pageNoIter;
		memcpy(bufferWriter, temp1->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
		bufferWriter = bufferWriter + MemoryBuffer::pagesize;
	}

	*((ID*)predicateWriter) = iter->first; predicateWriter = predicateWriter + sizeof(ID);
	*((size_t*)predicateWriter) = offset; predicateWriter = predicateWriter + sizeof(size_t);
	offset = offset + iter->second->meta->length[0];

	bufferWriter = buffer->resize(iter->second->meta->length[1]);
	char* startPos = bufferWriter + offset;

	pageNoIter = iter->second->usedPage[1].begin(); limit = iter->second->usedPage[1].end();
	for(; pageNoIter != limit; pageNoIter++ ) {
		size_t pageNo = *pageNoIter;
		memcpy(startPos, temp2->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
		startPos = startPos + MemoryBuffer::pagesize;
	}

	assert(iter->second->meta->length[1] == iter->second->usedPage[1].size() * MemoryBuffer::pagesize);
	offset = offset + iter->second->meta->length[1];

	iter++;
	for(; iter != predicate_managers[0].end(); iter++) {
		bufferWriter = buffer->resize(iter->second->meta->length[0]);
		startPos = bufferWriter + offset;

		pageNoIter = iter->second->usedPage[0].begin(); limit = iter->second->usedPage[0].end();

		for(; pageNoIter != limit; pageNoIter++) {
			size_t pageNo = *pageNoIter;
			memcpy(startPos, temp1->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos = startPos + MemoryBuffer::pagesize;
		}
		//cout<<"used page count: "<<iter->second->usedPage[0].size()<<endl;

		//iter->second->meta->endPtr[0] = startPos + iter->second->meta->usedSpace[0];  //used to build index;

		*((ID*)predicateWriter) = iter->first; predicateWriter = predicateWriter + sizeof(ID);
		*((size_t*)predicateWriter) = offset; predicateWriter = predicateWriter + sizeof(size_t);
		offset += iter->second->meta->length[0];

		assert(iter->second->usedPage[0].size() * MemoryBuffer::pagesize == iter->second->meta->length[0]);

		bufferWriter = buffer->resize(iter->second->meta->length[1]);
		startPos = bufferWriter + offset;
		//iter->second->meta->startPtr[1] = startPos; //used to build index;
		//iter->second->meta->endPtr[1] = startPos + iter->second->meta->usedSpace[1];
		pageNoIter = iter->second->usedPage[1].begin(); limit = iter->second->usedPage[1].end();
		for(; pageNoIter != limit; pageNoIter++) {
			size_t pageNo = *pageNoIter;
			memcpy(startPos, temp2->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos = startPos + MemoryBuffer::pagesize;
		}

		offset += iter->second->meta->length[1];
		assert(iter->second->usedPage[1].size() * MemoryBuffer::pagesize == iter->second->meta->length[1]);
	}

	buffer->flush();
	temp1->discard();
	temp2->discard();

	iter = predicate_managers[1].begin();
	for(; iter != predicate_managers[1].end(); iter++) {
		bufferWriter = buffer->resize(iter->second->meta->length[0]);
		startPos = bufferWriter + offset;

		pageNoIter = iter->second->usedPage[0].begin(); limit = iter->second->usedPage[0].end();
		for(; pageNoIter != limit; pageNoIter++) {
			size_t pageNo = *pageNoIter;
			memcpy(startPos, temp3->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos = startPos + MemoryBuffer::pagesize;
		}

		*((ID*)predicateWriter) = iter->first; predicateWriter = predicateWriter + sizeof(ID);
		*((size_t*)predicateWriter) = offset; predicateWriter = predicateWriter + sizeof(size_t);
		offset += iter->second->meta->length[0];

		assert(iter->second->usedPage[0].size() * MemoryBuffer::pagesize == iter->second->meta->length[0]);

		bufferWriter = buffer->resize(iter->second->usedPage[1].size() * MemoryBuffer::pagesize);
		startPos = bufferWriter + offset;

		pageNoIter = iter->second->usedPage[1].begin(); limit = iter->second->usedPage[1].end();
		for(; pageNoIter != limit; pageNoIter++) {
			size_t pageNo = *pageNoIter;
			memcpy(startPos, temp4->get_address() + pageNo * MemoryBuffer::pagesize, MemoryBuffer::pagesize);
			startPos = startPos + MemoryBuffer::pagesize;
		}

		offset += iter->second->meta->length[1];
		assert(iter->second->usedPage[1].size() * MemoryBuffer::pagesize == iter->second->meta->length[1]);
	}
	buffer->flush();
	predicateBuffer->flush();

	predicateWriter = predicateBuffer->get_address();
	int i = 0;

	ID id;
	for(iter = predicate_managers[0].begin(); iter != predicate_managers[0].end(); iter++, i++) {
		id = *((ID*)predicateWriter);
		assert(iter->first == id);
		predicateWriter = predicateWriter + sizeof(ID);
		offset = *((size_t*)predicateWriter);
		predicateWriter = predicateWriter + sizeof(size_t);

		char* base = buffer->get_address() + offset;
		iter->second->meta = (ChunkManagerMeta*)base;
		iter->second->meta->startPtr[0] = base + sizeof(ChunkManagerMeta);
		iter->second->meta->endPtr[0] = iter->second->meta->startPtr[0] + iter->second->meta->usedSpace[0];
		iter->second->meta->startPtr[1] = base + iter->second->meta->length[0];
		iter->second->meta->endPtr[1] = iter->second->meta->startPtr[1] + iter->second->meta->usedSpace[1];
		//::printMeta(*(iter->second->meta));
	}

	for(iter = predicate_managers[1].begin(); iter != predicate_managers[1].end(); iter++, i++) {
		id = *((ID*)predicateWriter);
		assert(iter->first == id);
		predicateWriter = predicateWriter + sizeof(ID);
		offset = *((size_t*)predicateWriter);
		predicateWriter = predicateWriter + sizeof(size_t);

		char* base = buffer->get_address() + offset;
		iter->second->meta = (ChunkManagerMeta*)base;
		iter->second->meta->startPtr[0] = base + sizeof(ChunkManagerMeta);
		iter->second->meta->endPtr[0] = iter->second->meta->startPtr[0] + iter->second->meta->usedSpace[0];
		iter->second->meta->startPtr[1] = base + iter->second->meta->length[0];
		iter->second->meta->endPtr[1] = iter->second->meta->startPtr[1] + iter->second->meta->usedSpace[1];
		//::printMeta(*(iter->second->meta));
	}

	temp3->discard();
	temp4->discard();

	//build index;
	MMapBuffer* bitmapIndex = NULL;
#ifdef DEBUG
	cout<<"build hash index for subject"<<endl;
#endif
	for ( map<ID,ChunkManager*>::iterator iter = predicate_managers[0].begin(); iter != predicate_managers[0].end(); iter++ ) {
		if ( iter->second != NULL ) {
#ifdef DEBUG
			cout<<iter->first<<endl;
#endif
			iter->second->buildChunkIndex();
			iter->second->getChunkIndex(1)->save(bitmapIndex);
			iter->second->getChunkIndex(2)->save(bitmapIndex);
		}
	}

#ifdef DEBUG
	cout<<"build hash index for object"<<endl;
#endif
	for ( map<ID, ChunkManager*>::iterator iter = predicate_managers[1].begin(); iter != predicate_managers[1].end(); iter++ ) {
		if ( iter->second != NULL ) {
#ifdef DEBUF
			cout<<iter->first<<endl;
#endif
			iter->second->buildChunkIndex();
			iter->second->getChunkIndex(1)->save(bitmapIndex);
			iter->second->getChunkIndex(2)->save(bitmapIndex);
		}
	}

	delete bitmapIndex;
	delete buffer;
	delete predicateBuffer;

}

BitmapBuffer*  BitmapBuffer::load(MMapBuffer* bitmapImage, MMapBuffer*& bitmapIndexImage, MMapBuffer* bitmapPredicateImage)
{
	//TODO load objects from image file;
	BitmapBuffer* buffer = new BitmapBuffer();

	char* predicateReader = bitmapPredicateImage->getBuffer();

	size_t predicateSize = bitmapPredicateImage->getSize() / ((sizeof(ID) + sizeof(size_t)) * 2);
	ID id;
	size_t offset = 0, indexOffset = 0;
	for(size_t i = 0; i < predicateSize; i++) {
		id = *((ID*)predicateReader); predicateReader = predicateReader + sizeof(ID);
		predicateReader = predicateReader + sizeof(size_t);
		ChunkManager* manager = ChunkManager::load(id, 0, bitmapImage->getBuffer(), offset);
		manager->chunkIndex[0] = LineHashIndex::load(*manager, LineHashIndex::SUBJECT_INDEX, bitmapIndexImage->getBuffer(), indexOffset);
		manager->chunkIndex[1] = LineHashIndex::load(*manager, LineHashIndex::SUBJECT_INDEX, bitmapIndexImage->getBuffer(), indexOffset);

		buffer->predicate_managers[0][id] = manager;
	}

	for(size_t i = 0; i < predicateSize; i++) {
		id = *((ID*)predicateReader); predicateReader = predicateReader + sizeof(ID);
		offset = *((size_t*)predicateReader);
		predicateReader = predicateReader + sizeof(size_t);

		ChunkManager* manager = ChunkManager::load(id, 1, bitmapImage->getBuffer(), offset);
		manager->chunkIndex[0] = LineHashIndex::load(*manager, LineHashIndex::OBJECT_INDEX, bitmapIndexImage->getBuffer(), indexOffset);
		manager->chunkIndex[1] = LineHashIndex::load(*manager, LineHashIndex::OBJECT_INDEX, bitmapIndexImage->getBuffer(), indexOffset);

		buffer->predicate_managers[1][id] = manager;
	}

	return buffer;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void getTempFilename(string& filename, unsigned pid, unsigned _type) {
	filename.clear();
	filename.append(DATABASE_PATH);
	filename.append("temp_");
	char temp[5];
	sprintf(temp, "%d", pid);
	filename.append(temp);
	sprintf(temp, "%d", _type);
	filename.append(temp);
}

ChunkManager::ChunkManager(unsigned pid, unsigned _type, BitmapBuffer* _bitmapBuffer) : bitmapBuffer(_bitmapBuffer) {
	/*string filename;
	getTempFilename(filename, pid, _type);
	filename.append("_0");
	ptrs[0] = new MMapBuffer(filename.c_str(), sizeof(ChunkManagerMeta) + INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	getTempFilename(filename, pid, _type);
	filename.append("_1");
	ptrs[1] = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);*/
	usedPage[0].resize(0); usedPage[1].resize(0);
	size_t pageNo = 0;
	meta = NULL;
	ptrs[0] = bitmapBuffer->getPage(_type, 0, pageNo);
	usedPage[0].push_back(pageNo);
	ptrs[1] = bitmapBuffer->getPage(_type, 1, pageNo);
	usedPage[1].push_back(pageNo);

	assert(ptrs[1] != ptrs[0]);

	meta = (ChunkManagerMeta*)ptrs[0];
	memset((char*)meta, 0, sizeof(ChunkManagerMeta));
	meta->endPtr[0] = meta->startPtr[0] = ptrs[0] + sizeof(ChunkManagerMeta);
	meta->endPtr[1] = meta->startPtr[1] = ptrs[1];
	meta->length[0] = usedPage[0].size() * MemoryBuffer::pagesize;
	meta->length[1] = usedPage[1].size() * MemoryBuffer::pagesize;
	meta->usedSpace[0] = 0;
	meta->usedSpace[1] = 0;
	meta->tripleCount[0] = meta->tripleCount[1] = 0;
	meta->pid = pid;
	meta->type = _type;

	//need to modify!
	if( meta->type == 0) {
		chunkIndex[0] = new LineHashIndex(*this, LineHashIndex::SUBJECT_INDEX);
		chunkIndex[1] = new LineHashIndex(*this, LineHashIndex::SUBJECT_INDEX);
	} else {
		chunkIndex[0] = new LineHashIndex(*this, LineHashIndex::OBJECT_INDEX);
		chunkIndex[1] = new LineHashIndex(*this, LineHashIndex::OBJECT_INDEX);
	}

	for (int i = 0; i < 2; i++)
		meta->tripleCount[i] = 0;

}

ChunkManager::~ChunkManager() {
	// TODO Auto-generated destructor stub
	///free the buffer;
	ptrs[0] = ptrs[1] = NULL;

	if(chunkIndex[0] != NULL)
		delete chunkIndex[0];
	chunkIndex[0] = NULL;
	if(chunkIndex[1] != NULL)
		delete chunkIndex[1];
	chunkIndex[1] = NULL;
}

Status ChunkManager::deleteChunk() {
	// TODO
	return OK;
}

static void getInsertChars(char* temp, unsigned x, unsigned y) {
	char* ptr = temp;

	while (x >= 128) {
		unsigned char c = static_cast<unsigned char> (x & 127);
		*ptr = c;
		ptr++;
		x >>= 7;
	}
	*ptr = static_cast<unsigned char> (x & 127);
	ptr++;

	while (y >= 128) {
		unsigned char c = static_cast<unsigned char> (y | 128);
		*ptr = c;
		ptr++;
		y >>= 7;
	}
	*ptr = static_cast<unsigned char> (y | 128);
	ptr++;
}

void ChunkManager::insertXY(unsigned x, unsigned y, unsigned len, unsigned char type)
{
	char temp[10];
	getInsertChars(temp, x, y);

	unsigned offset = 0;
	unsigned remain = len;
	if(isPtrFull(type, len) == true) {
		if(type == 1) {
			offset = meta->length[0] - meta->usedSpace[0] - sizeof(ChunkManagerMeta);
		} else {
			offset = meta->length[1] - meta->usedSpace[1];
		}

		memcpy(meta->endPtr[type - 1], temp, offset);
		remain = len - offset;
		//cout<<"resize"<<endl;
		resize(type);
		//cout<<"resize"<<endl;
	}

	memcpy(meta->endPtr[type - 1], temp + offset, remain);

	meta->endPtr[type - 1] = meta->endPtr[type - 1] + remain;
	meta->usedSpace[type - 1] = meta->usedSpace[type - 1] + len;
	tripleCountAdd(type);
}

Status ChunkManager::resize(unsigned char type) {
	// TODO
	size_t pageNo = 0;
	ptrs[type - 1] = bitmapBuffer->getPage(meta->type, type - 1, pageNo);
	usedPage[type - 1].push_back(pageNo);
	meta->length[type - 1] = usedPage[type - 1].size() * MemoryBuffer::pagesize;
	meta->endPtr[type - 1] = ptrs[type - 1];

	bufferCount++;
	return OK;
}

Status ChunkManager::optimize() {
	// TODO
	return OK;
}

/// build the hash index for query;
Status ChunkManager::buildChunkIndex()
{
	chunkIndex[0]->buildIndex(1);
	chunkIndex[1]->buildIndex(2);

	return OK;
}

Status ChunkManager::getChunkPosByID(ID id, unsigned typeID, unsigned& offset)
{
	if(typeID == 1) {
		return chunkIndex[0]->getOffsetByID(id, offset, typeID);
	}else if(typeID  == 2) {
		return chunkIndex[1]->getOffsetByID(id, offset, typeID);
	}

	cerr<<"unknown type id"<<endl;
	return ERROR;
}

void ChunkManager::setColStartAndEnd(ID& startColID)
{
}

int ChunkManager::findChunkPosByPtr(char* chunkPtr, int& offSet)
{
	return -1;
}

bool ChunkManager::isPtrFull(unsigned char type, unsigned len)
{
	if(type == 1) {
		len = len + sizeof(ChunkManagerMeta);
	}
	return meta->usedSpace[type - 1] + len >= meta->length[type - 1];
}

void ChunkManager::save(ofstream& ofile)
{

}

ChunkManager* ChunkManager::load(unsigned pid, unsigned type, char* buffer, size_t& offset)
{
	ChunkManagerMeta * meta = (ChunkManagerMeta*)(buffer + offset);
	if(meta->pid != pid || meta->type != type) {
		MessageEngine::showMessage("load chunkmanager error: check meta info", MessageEngine::ERROR);
		cout<<meta->pid<<": "<<meta->type<<endl;
		return NULL;
	}

	ChunkManager* manager = new ChunkManager();
	char* base = buffer + offset + sizeof(ChunkManagerMeta);
	manager->meta = meta;
	manager->meta->startPtr[0] = base; manager->meta->startPtr[1] = buffer + offset + manager->meta->length[0];
	manager->meta->endPtr[0] = manager->meta->startPtr[0] + manager->meta->usedSpace[0];
	manager->meta->endPtr[1] = manager->meta->startPtr[1] + manager->meta->usedSpace[1];

	offset = offset + manager->meta->length[0] + manager->meta->length[1];

	return manager;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

Chunk::Chunk(unsigned char type, ID xMax, ID xMin, ID yMax, ID yMin, char* startPtr, char* endPtr) {
	// TODO Auto-generated constructor stub
	this->type = type;
	this->xMax = xMax;
	this->xMin = xMin;
	this->yMax = yMax;
	this->yMin = yMin;
	count = 0;
	this->startPtr = startPtr;
	this->endPtr = endPtr;
	this->flagVector = new BitVectorWAH;
}

Chunk::Chunk(unsigned char type,ID xMax, ID xMin, ID yMax, ID yMin, char* startPtr, char* endPtr, BitVectorWAH* flagVector)
{
	this->type = type;
	this->xMax = xMax;
	this->xMin = xMin;
	this->yMax = yMax;
	this->yMin = yMin;
	this->startPtr = startPtr;
	this->endPtr = endPtr;
	this->flagVector = flagVector;
}

Chunk::~Chunk() {
	// TODO Auto-generated destructor stub
	this->startPtr = 0;
	this->endPtr = 0;
	delete flagVector;
	flagVector = NULL;
}

/*
 *	write x id; set the 7th bit to 0 to indicate it is a x byte;
 */
void Chunk::writeXId(ID id, char*& ptr) {
// Write a id
	while (id >= 128) {
		unsigned char c = static_cast<unsigned char> (id & 127);
		*ptr = c;
		ptr++;
		id >>= 7;
	}
	*ptr = static_cast<unsigned char> (id & 127);
	ptr++;
}

/*
 *	write y id; set the 7th bit to 1 to indicate it is a y byte;
 */
void Chunk::writeYId(ID id, char*& ptr) {
	while (id >= 128) {
		unsigned char c = static_cast<unsigned char> (id | 128);
		*ptr = c;
		ptr++;
		id >>= 7;
	}
	*ptr = static_cast<unsigned char> (id | 128);
	ptr++;
}

Status Chunk::insertXY(ID xId, ID yId, unsigned char xLen, unsigned char yLen,bool flag) {
	if(isChunkFull())
		return CHUNK_IS_FULL;
	//mem write xId yId by xLen and yLen
	memcpy(endPtr,&xId,xLen);
	endPtr += xLen;
	memcpy(endPtr,&yId,yLen);
	endPtr += yLen;
	//modify xMin xMax yMax yMin
	//maybe there is useless
	xMax = xId > xMax ? xId : xMax;
	yMax = yId > yMax ? yId : yMax;
	xMin = xId < xMin ? xId : xMin;
	yMin = yId < yMin ? yId : yMin;

	soFlags->push_back(flag);

	return OK;
}

Status Chunk::insertXY(ID xId, ID yId, bool flag) {
	//if(isChunkFull())
	//	return CHUNK_IS_FULL;

	//modify xMin xMax yMax yMin
	//maybe there is useless
	xMax = xId > xMax ? xId : xMax;
	yMax = yId > yMax ? yId : yMax;
	xMin = xId < xMin ? xId : xMin;
	yMin = yId < yMin ? yId : yMin;

	writeXId(xId, endPtr);
	writeYId(yId, endPtr);

	addCount();

	return OK;
}

Status Chunk::completeInsert(ID& startColID)
{
//	flagVector->completeInsert();

	//set the start and end column ID

	//this->colStart = startColID;
	//this->colEnd = startColID + (endPtr - startPtr) / Type_2_Length(this->type) - 1;

	//startColID = this->colEnd + 1;
/*
	if ( flagVector != NULL)
		delete flagVector;

	flagVector = BitVectorWAH::convertVector(*soFlags);

	delete soFlags;
*/
	return OK;
}

/*
 * pos begins from 1
 *
 */
Status Chunk::getSO(ID& subjectID,ID& objectID, ID pos)
{
	if( pos > (colEnd - colStart + 1))
		return ERROR;
	unsigned char xLen, yLen;
	ID x = 0,y = 0;

	char* p = startPtr + (pos - 1) * Type_2_Length(this->type);
	Type_2_Length(type,xLen,yLen);

	memcpy(&x,p,xLen);
	p = p + xLen;
	memcpy(&y,p,yLen);

	bool flag = flagVector->getValue(pos);

//	bool flag = flagVector[pos-1];

	if(flag == true){
		subjectID = x+y;
		objectID = x;
	}else{
		objectID = x+y;
		subjectID = x;
	}

	return OK;
}

static inline unsigned int readUInt(const uchar* reader) {
	return (reader[0]<<24 | reader[1] << 16 | reader[2] << 8 | reader[3]);
}

const uchar* Chunk::readXId(const uchar* reader, register ID& id) {
#ifdef WORD_ALIGN
	id = 0;
	register unsigned int c = *((unsigned int*)reader);
	register unsigned int flag = c & 0x80808080;       	/* get the first bit of every byte. */
	switch(flag) {
		case 0:		//reads 4 or more bytes;
			id = *reader;
			reader++;
			id = id | ((*reader) << 7);
			reader++;
			id = id | ((*reader) << 14);
			reader++;
			id = id | ((*reader) << 21);
			reader++;
			if(*reader < 128) {
				id = id | ((*reader) << 28);
				reader++;
			}
			break;
		case 0x80000080:
		case 0x808080:
		case 0x800080:
		case 0x80008080:
		case 0x80:
		case 0x8080:
		case 0x80800080:
		case 0x80808080:
			break;

		case 0x80808000://reads 1 byte;
		case 0x808000:
		case 0x8000:
		case 0x80008000:
			id = *reader;
			reader++;
			break;
		case 0x800000: //read 2 bytes;
		case 0x80800000:
			id = *reader;
			reader++;
			id = id | ((*reader) << 7);
			reader++;
			break;
		case 0x80000000: //reads 3 bytes;
			id = *reader;
			reader++;
			id = id | ((*reader) << 7);
			reader++;
			id = id | ((*reader) << 14);
			reader++;
			break;
	}
	return reader;
#else
// Read an x id
	register unsigned shift = 0;
	id = 0;
	register unsigned int c;

	while (true) {
		c = *reader;
		if (!(c & 128)) {
			id |= c << shift;
			shift += 7;
		} else {
			break;
		}
		reader++;
	}
	return reader;
#endif /* end for WORD_ALIGN */
}

const uchar* Chunk::readYId(const uchar* reader, register ID& id) {
// Read an y id
#ifdef WORD_ALIGN
	id = 0;
	register unsigned int c = *((unsigned int*)reader);
	register unsigned int flag = c & 0x80808080;       /* get the first bit of every byte. */
	switch(flag) {
		case 0: //no byte;
		case 0x8000:
		case 0x808000:
		case 0x80008000:
		case 0x80800000:
		case 0x800000:
		case 0x80000000:
		case 0x80808000:
			break;
		case 0x80:
		case 0x80800080:
		case 0x80000080:
		case 0x800080: //one byte
			id = (*reader)& 0x7F;
			reader++;
			break;
		case 0x8080:
		case 0x80008080: // two bytes
			id = (*reader)& 0x7F;
			reader++;
			id = id | (((*reader) & 0x7F) << 7);
			reader++;
			break;
		case 0x808080: //three bytes;
			id = (*reader) & 0x7F;
			reader++;
			id = id | (((*reader) & 0x7F) << 7);
			reader++;
			id = id | (((*reader) & 0x7F) << 14);
			reader++;
			break;
		case 0x80808080: //reads 4 or 5 bytes;
			id = (*reader) & 0x7F;
			reader++;
			id = id | (((*reader) & 0x7F) << 7);
			reader++;
			id = id | (((*reader) & 0x7F) << 14);
			reader++;
			id = id | (((*reader) & 0x7F) << 21);
			reader++;
			if(*reader >= 128) {
				id = id | (((*reader) & 0x7F) << 28);
				reader++;
			}
			break;
	}
	return reader;
#else
	register unsigned shift = 0;
	id = 0;
	register unsigned int c;

	while (true) {
		c = *reader;
		if (c & 128) {
			id |= (c & 0x7F) << shift;
			shift += 7;
		} else {
			break;
		}
		reader++;
	}
	return reader;
#endif /* END FOR WORD_ALIGN */
}

const uchar* Chunk::skipId(const uchar* reader, unsigned char flag) {
// Skip an id
	if(flag == 1) {
		while ((*reader) & 128)
			++reader;
	//	return reader;
	} else {
		while (!((*reader) & 128))
			++reader;
	//	return reader;
	}

	return reader;
}

const uchar* Chunk::skipForward(const uchar* reader) {
// skip a x,y forward;
	return skipId(skipId(reader, 0), 1);
}

const uchar* Chunk::skipBackward(const uchar* reader) {
// skip backward to the last x,y;
	while ((*reader) == 0)
		--reader;
	while ((*reader) & 128)
		--reader;
	while (!((*reader) & 128))
		--reader;
	return ++reader;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////

Status ChunkIndex::buildChunkIndex(unsigned chunkType) {
	if(idTable == NULL) {
		idTable = new MemoryBuffer(HASH_CAPACITY);
		tableSize = idTable->getSize() / sizeof(unsigned);
		offsetTable = new MemoryBuffer(HASH_CAPACITY);
		idTableEntries = (ID*)idTable->getBuffer();
		offsetTableEntries = (ID*)offsetTable->getBuffer();
		tableSize = 0;
	}

	const uchar* begin, *limit, *reader;
	ID x,y;

	if(chunkType == 1) {
		reader = chunkManager.getStartPtr(1);
		begin = reader;
		if(chunkManager.getStartPtr(chunkType) == chunkManager.getEndPtr(chunkType))
			return OK;

		x = 0;
		reader = Chunk::readXId(reader, x);
		insertEntries(x, 0);

		reader = reader + (int)MemoryBuffer::pagesize;

		limit = chunkManager.getEndPtr(1);
		while(reader < limit) {
			x = 0;
			//const char* temp = this->backSkip(reader);
			const uchar* temp = Chunk::skipBackward(reader);
			Chunk::readXId(temp, x);
			insertEntries(x, temp - begin);

			reader = reader + (int)MemoryBuffer::pagesize;
		}

		//reader = backSkip(limit);
		reader = Chunk::skipBackward(limit);
		x = 0;
		Chunk::readXId(reader, x);
		insertEntries(x, reader - begin);
	}

	if(chunkType == 2) {
		reader = chunkManager.getStartPtr(2);
		begin = reader;
		if(chunkManager.getStartPtr(chunkType) == chunkManager.getEndPtr(chunkType))
			return OK;

		x = 0; y = 0;
		Chunk::readYId(Chunk::readXId(reader, x), y);
		insertEntries(x + y, 0);

		reader = reader + (int)MemoryBuffer::pagesize;

		limit = chunkManager.getEndPtr(2);
		while(reader < limit) {
			x = 0; y = 0;
			//const char* temp = this->backSkip(reader);
			const uchar* temp = Chunk::skipBackward(reader);
			Chunk::readYId(Chunk::readXId(temp, x), y);
			insertEntries(x + y, temp - begin);

			reader = reader + (int)MemoryBuffer::pagesize;
		}

		x= y = 0;
		//reader = backSkip(limit);
		reader = Chunk::skipBackward(limit);
		Chunk::readYId(Chunk::readXId(reader, x), y);
		insertEntries(x + y, reader - begin);
	}

	return OK;
}

void ChunkIndex::insertEntries(ID id, unsigned offset)
{
	if(isBufferFull() == true) {
		idTable->resize(HASH_CAPACITY);
		idTableEntries = (ID*)idTable->get_address();
		offsetTable->resize(HASH_CAPACITY);
		offsetTableEntries = (ID*)offsetTable->get_address();
	}
	idTableEntries[tableSize] = id;
	offsetTableEntries[tableSize] = offset;

	tableSize++;
}

const char* ChunkIndex::backSkip(const char* reader)
{
	if((*reinterpret_cast<const unsigned char*> (reader)) & 128 ) { //encounter y;
		while ((*reinterpret_cast<const unsigned char*> (reader)) & 128)
			--reader;
		while (!((*reinterpret_cast<const unsigned char*> (reader)) & 128))
			--reader;
	} else { //encounter x;
		while (!((*reinterpret_cast<const unsigned char*> (reader)) & 128))
			--reader;
	}

	return ++reader;
}

int ChunkIndex::searchChunk(ID id) {
	int low, high, mid;
	low = 0;
	high = tableSize - 1;

	while (low <= high) {
		mid = low + (high - low) / 2;

		if (id <= idTableEntries[mid + 1] && id >= idTableEntries[mid]) {
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
			low = mid + 1;
	}

	return -1;

}

Status ChunkIndex::getOffsetById(ID id, unsigned int& offset, unsigned int typeID) {
	int offsetId = this->searchChunk(id);
	if(offsetId == -1) {
		//cout<<"id: "<<id<<endl;
		if(tableSize > 0 && id > idTableEntries[tableSize - 1]) {
			return NOT_FOUND;
		} else {
			offset = 0;
			return OK;
		}
	}

	/*if(idTableEntries[offsetId] == id) {
		offset = offsetTableEntries[offsetId];
		return OK;
	}*/

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
				if (x < id) {
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
			x = 0;
			mid = low + (high - low) / 2;
			if(lastmid == mid)
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
					if (x < id) {
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
		}
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
				//x = 0; y = 0;
				reader = Chunk::skipBackward(beginPtr + low);
				Chunk::readYId(Chunk::readXId(reader, x), y);
				if (x + y < id) {
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
			if(lastmid == mid)
				break;
			lastmid = mid;
			reader = Chunk::readXId(reader, x);
			reader = Chunk::readYId(reader, y);
			if (x + y == id) {
				lastmid = mid;
				while(mid > 0) {
					//x = y = 0;
					reader = Chunk::skipBackward(beginPtr + mid);
					Chunk::readYId(Chunk::readXId(reader, x), y);
					if(x + y < id) {
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
				low = mid + 1;
			}
		}
	}
	if (mid <= 0)
		offset = 0;
	else
		//if not found, offset is the first id which is bigger than the given id.
		offset = Chunk::skipBackward(beginPtr + mid) - beginPtr;

	return OK;
}

bool ChunkIndex::isBufferFull() {
	return tableSize >= idTable->getSize() / 4;
}

Status ChunkIndex::save(MMapBuffer*& indexBuffer)
{
	char* writeBuf;

	if(indexBuffer == NULL) {
		indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/BitmapBuffer_index").c_str(), tableSize * 4 * 2 + 4);
		writeBuf = indexBuffer->get_address();
	} else {
		size_t size = indexBuffer->getSize();
		writeBuf = indexBuffer->resize(tableSize * 4 * 2 + 4) + size;
	}

	*(ID*)writeBuf = tableSize;
	writeBuf = writeBuf + 4;
	memcpy(writeBuf, (char*)idTableEntries, tableSize * 4);
	writeBuf = writeBuf + tableSize * 4;
	memcpy(writeBuf, (char*)offsetTableEntries, tableSize * 4);

	indexBuffer->flush();
	delete idTable;
	idTable = NULL;
	delete offsetTable;
	offsetTable = NULL;

	return OK;
}

ChunkIndex* ChunkIndex::load(ChunkManager& manager, IndexType indexType, char* buffer, size_t& offset)
{
	ChunkIndex* index = new ChunkIndex(manager, indexType);
	char* base = buffer + offset;
	index->tableSize = *((ID*)base);
	index->idTableEntries = (ID*)(base + 4);
	index->offsetTableEntries = (ID*)(index->idTableEntries + index->tableSize);
	offset = offset + 4 + 4 * 2 * index->tableSize;

	return index;
}
