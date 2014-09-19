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

#include "StatisticsBuffer.h"
#include "BitmapBuffer.h"
#include "MMapBuffer.h"
#include "HashIndex.h"
#include "EntityIDBuffer.h"
#include "URITable.h"
#include "MemoryBuffer.h"

extern char* writeData(char* writer, unsigned data);
extern const char* readData(const char* reader, unsigned int& data);

static inline unsigned readDelta1(const unsigned char* pos) { return pos[0]; }
static unsigned readDelta2(const unsigned char* pos) { return (pos[0]<<8)|pos[1]; }
static unsigned readDelta3(const unsigned char* pos) { return (pos[0]<<16)|(pos[1]<<8)|pos[2]; }
static unsigned readDelta4(const unsigned char* pos) { return (pos[0]<<24)|(pos[1]<<16)|(pos[2]<<8)|pos[3]; }

static void writeUint32(unsigned char* target,unsigned value)
   // Write a 32bit value
{
   target[0]=value>>24;
   target[1]=(value>>16)&0xFF;
   target[2]=(value>>8)&0xFF;
   target[3]=value&0xFF;
}

static unsigned char* writeDelta0(unsigned char* buffer, unsigned value)
   // Write an integer with varying size
{
	if (value >= (1 << 24)) {
		writeUint32(buffer, value);
		return buffer + 4;
	} else if (value >= (1 << 16)) {
		buffer[0] = value >> 16;
		buffer[1] = (value >> 8) & 0xFF;
		buffer[2] = value & 0xFF;
		return buffer + 3;
	} else if (value >= (1 << 8)) {
		buffer[0] = value >> 8;
		buffer[1] = value & 0xFF;
		return buffer + 2;
	} else if (value > 0) {
		buffer[0] = value;
		return buffer + 1;
	} else
		return buffer;
}

StatisticsBuffer::StatisticsBuffer() : HEADSPACE(2) {
	// TODO Auto-generated constructor stub
	
}

StatisticsBuffer::~StatisticsBuffer() {
	// TODO Auto-generated destructor stub
}

/////////////////////////////////////////////////////////////////

OneConstantStatisticsBuffer::OneConstantStatisticsBuffer(const string path, StatisticsType type) : StatisticsBuffer(), type(type), reader(NULL), ID_HASH(50)
{
	buffer = new MMapBuffer(path.c_str(), STATISTICS_BUFFER_INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	writer = (unsigned char*)buffer->getBuffer();
	index.resize(2000);
	nextHashValue = 0;
	lastId = 0;
	usedSpace = 0;
	reader = NULL;

	//triples = new Triple[ID_HASH];
	triples = (Triple *)malloc(sizeof(Triple) * ID_HASH * 4);
	first = true;
}

OneConstantStatisticsBuffer::~OneConstantStatisticsBuffer()
{
	if(buffer != NULL) {
		delete buffer;
	}
	buffer = NULL;

	/*if(triples != NULL) {
		delete[] triples;
		triples = NULL;
	}*/
	free(triples);
}

void OneConstantStatisticsBuffer::writeId(unsigned id, char*& ptr, bool isID)
{
	if ( isID == true ) {
		while (id >= 128) {
			unsigned char c = static_cast<unsigned char> (id & 127);
			*ptr = c;
			ptr++;
			id >>= 7;
		}
		*ptr = static_cast<unsigned char> (id & 127);
		ptr++;
	} else {
		while (id >= 128) {
			unsigned char c = static_cast<unsigned char> (id | 128);
			*ptr = c;
			ptr++;
			id >>= 7;
		}
		*ptr = static_cast<unsigned char> (id | 128);
		ptr++;
	}
}

bool OneConstantStatisticsBuffer::isPtrFull(unsigned len) 
{
	return (unsigned int) ( writer - (unsigned char*)buffer->getBuffer() + len ) > buffer->getSize() ? true : false;
}

unsigned OneConstantStatisticsBuffer::getLen(unsigned v)
{
	if (v >= (1 << 24))
		return 4;
	else if (v >= (1 << 16))
		return 3;
	else if (v >= (1 << 8))
		return 2;
	else if(v > 0)
		return 1;
	else
		return 0;
}

static unsigned int countEntity(const unsigned char* begin, const unsigned char* end)
{
	if(begin >= end) 
		return 0;

	unsigned int entityCount = 1;
	begin = begin + 8;

	while(begin < end) {
		// Decode the header byte
		unsigned info = *(begin++);
		// Small gap only?
		if (info < 0x80) {
			if (!info)
				break;
			/*
			count = (info >> 4) + 1;
			value1 += (info & 15);
			(*writer).value1 = value1;
			(*writer).count = count;
			++writer;
			*/
			entityCount++ ;
			continue;
		}
		// Decode the parts
		//value1 += 1;
		switch (info & 127) {
			case 0: break;
			case 1: begin += 1; break;
			case 2: begin += 2;break;
			case 3: begin += 3;break;
			case 4: begin += 4;break;
			case 5: begin += 1;break;
			case 6: begin += 2;break;
			case 7: begin += 3;break;
			case 8: begin += 4;break;
			case 9: begin += 5; break;
			case 10: begin += 2; break;
			case 11: begin += 3; break;
			case 12: begin += 4; break;
			case 13: begin += 5; break;
			case 14: begin += 6; break;
			case 15: begin += 3; break;
			case 16: begin += 4; break;
			case 17: begin += 5; break;
			case 18: begin += 6; break;
			case 19: begin += 7;break;
			case 20: begin += 4;break;
			case 21: begin += 5;break;
			case 22: begin += 6;break;
			case 23: begin += 7;break;
			case 24: begin += 8;break;
		}
		entityCount++;
	}

	return entityCount;
}

const unsigned char* OneConstantStatisticsBuffer::decode(const unsigned char* begin, const unsigned char* end)
{
	Triple* writer = triples;
	unsigned value1, count;
	value1 = readDelta4(begin);
	begin += 4;
	count = readDelta4(begin);
	begin += 4;

	(*writer).value1 = value1;
	(*writer).count = count;
	writer++;

	while (begin < end) {
		// Decode the header byte
		unsigned info = *(begin++);
		// Small gap only?
		if (info < 0x80) {
			if (!info)
				break;
			count = (info >> 4) + 1;
			value1 += (info & 15);
			(*writer).value1 = value1;
			(*writer).count = count;
			++writer;
			continue;
		}
		// Decode the parts
		value1 += 1;
		switch (info & 127) {
			case 0: count = 1;break;
			case 1: count = readDelta1(begin) + 1; begin += 1; break;
			case 2: count = readDelta2(begin) + 1;begin += 2;break;
			case 3: count = readDelta3(begin) + 1;begin += 3;break;
			case 4: count = readDelta4(begin) + 1;begin += 4;break;
			case 5: value1 += readDelta1(begin);count = 1;begin += 1;break;
			case 6: value1 += readDelta1(begin);count = readDelta1(begin + 1) + 1;begin += 2;break;
			case 7: value1 += readDelta1(begin);count = readDelta2(begin + 1) + 1;begin += 3;break;
			case 8: value1 += readDelta1(begin);count = readDelta3(begin + 1) + 1;begin += 4;break;
			case 9: value1 += readDelta1(begin); count = readDelta4(begin + 1) + 1; begin += 5; break;
			case 10: value1 += readDelta2(begin); count = 1; begin += 2; break;
			case 11: value1 += readDelta2(begin); count = readDelta1(begin + 2) + 1; begin += 3; break;
			case 12: value1 += readDelta2(begin); count = readDelta2(begin + 2) + 1; begin += 4; break;
			case 13: value1 += readDelta2(begin); count = readDelta3(begin + 2) + 1; begin += 5; break;
			case 14: value1 += readDelta2(begin); count = readDelta4(begin + 2) + 1; begin += 6; break;
			case 15: value1 += readDelta3(begin); count = 1; begin += 3; break;
			case 16: value1 += readDelta3(begin); count = readDelta1(begin + 3) + 1; begin += 4; break;
			case 17: value1 += readDelta3(begin); count = readDelta2(begin + 3) + 1; begin += 5; break;
			case 18: value1 += readDelta3(begin); count = readDelta3(begin + 3) + 1; begin += 6; break;
			case 19: value1 += readDelta3(begin);count = readDelta4(begin + 3) + 1;begin += 7;break;
			case 20: value1 += readDelta4(begin);count = 1;begin += 4;break;
			case 21: value1 += readDelta4(begin);count = readDelta1(begin + 4) + 1;begin += 5;break;
			case 22: value1 += readDelta4(begin);count = readDelta2(begin + 4) + 1;begin += 6;break;
			case 23: value1 += readDelta4(begin);count = readDelta3(begin + 4) + 1;begin += 7;break;
			case 24: value1 += readDelta4(begin);count = readDelta4(begin + 4) + 1;begin += 8;break;
		}
		(*writer).value1 = value1;
		(*writer).count = count;
		++writer;
	}

	pos = triples;
	posLimit = writer;

	return begin;
}

const unsigned char* OneConstantStatisticsBuffer::decodeID(const unsigned char* begin, const unsigned char* end)
{
	Triple* writer = triples;
	unsigned value1;
	value1 = readDelta4(begin);
	begin += 8;

	(*writer).value1 = value1;
	writer++;

	while (begin < end) {
		// Decode the header byte
		unsigned info = *(begin++);
		// Small gap only?
		if (info < 0x80) {
			if (!info)
				break;
			value1 += (info & 15);
			(*writer).value1 = value1;
			++writer;
			continue;
		}
		// Decode the parts
		value1 += 1;
		switch (info & 127) {
			case 0: break;
			case 1: begin += 1; break;
			case 2: begin += 2;break;
			case 3: begin += 3;break;
			case 4: begin += 4;break;
			case 5: value1 += readDelta1(begin);begin += 1;break;
			case 6: value1 += readDelta1(begin);begin += 2;break;
			case 7: value1 += readDelta1(begin);begin += 3;break;
			case 8: value1 += readDelta1(begin);begin += 4;break;
			case 9: value1 += readDelta1(begin);begin += 5; break;
			case 10: value1 += readDelta2(begin);begin += 2; break;
			case 11: value1 += readDelta2(begin);begin += 3; break;
			case 12: value1 += readDelta2(begin);begin += 4; break;
			case 13: value1 += readDelta2(begin);begin += 5; break;
			case 14: value1 += readDelta2(begin);begin += 6; break;
			case 15: value1 += readDelta3(begin);begin += 3; break;
			case 16: value1 += readDelta3(begin);begin += 4; break;
			case 17: value1 += readDelta3(begin);begin += 5; break;
			case 18: value1 += readDelta3(begin);begin += 6; break;
			case 19: value1 += readDelta3(begin);begin += 7;break;
			case 20: value1 += readDelta4(begin);begin += 4;break;
			case 21: value1 += readDelta4(begin);begin += 5;break;
			case 22: value1 += readDelta4(begin);begin += 6;break;
			case 23: value1 += readDelta4(begin);begin += 7;break;
			case 24: value1 += readDelta4(begin);begin += 8;break;
		}
		(*writer).value1 = value1;
		++writer;
	}

	pos = triples;
	posLimit = writer;
	return begin;
}

Status OneConstantStatisticsBuffer::decodeAndInsertID(const unsigned char* begin, const unsigned char* end, EntityIDBuffer *entBuffer)
{
	unsigned value1;
	value1 = readDelta4(begin);
	begin += 8;
	entBuffer->insertID(value1);

	while (begin < end) {
		// Decode the header byte
		unsigned info = *(begin++);
		// Small gap only?
		if (info < 0x80) {
			if (!info)
				break;
			value1 += (info & 15);
			entBuffer->insertID(value1);
			continue;
		}
		// Decode the parts
		value1 += 1;
		switch (info & 127) {
			case 0: break;
			case 1: begin += 1; break;
			case 2: begin += 2;break;
			case 3: begin += 3;break;
			case 4: begin += 4;break;
			case 5: value1 += readDelta1(begin);begin += 1;break;
			case 6: value1 += readDelta1(begin);begin += 2;break;
			case 7: value1 += readDelta1(begin);begin += 3;break;
			case 8: value1 += readDelta1(begin);begin += 4;break;
			case 9: value1 += readDelta1(begin);begin += 5; break;
			case 10: value1 += readDelta2(begin);begin += 2; break;
			case 11: value1 += readDelta2(begin);begin += 3; break;
			case 12: value1 += readDelta2(begin);begin += 4; break;
			case 13: value1 += readDelta2(begin);begin += 5; break;
			case 14: value1 += readDelta2(begin);begin += 6; break;
			case 15: value1 += readDelta3(begin);begin += 3; break;
			case 16: value1 += readDelta3(begin);begin += 4; break;
			case 17: value1 += readDelta3(begin);begin += 5; break;
			case 18: value1 += readDelta3(begin);begin += 6; break;
			case 19: value1 += readDelta3(begin);begin += 7;break;
			case 20: value1 += readDelta4(begin);begin += 4;break;
			case 21: value1 += readDelta4(begin);begin += 5;break;
			case 22: value1 += readDelta4(begin);begin += 6;break;
			case 23: value1 += readDelta4(begin);begin += 7;break;
			case 24: value1 += readDelta4(begin);begin += 8;break;
		}
		entBuffer->insertID(value1);
	}

	return OK;
}

unsigned int OneConstantStatisticsBuffer::getEntityCount()
{
	unsigned int entityCount = 0;
	unsigned i = 0;

	const unsigned char* begin, *end;
	unsigned beginChunk = 0, endChunk = 0;

#ifdef DEBUG
	cout<<"indexSize: "<<indexSize<<endl;
#endif
	for(i = 1; i <= indexSize; i++) {
		if(i < indexSize)
			endChunk = index[i];

		while(endChunk == 0 && i < indexSize) {
			i++;
			endChunk = index[i];
		}
		
		if(i == indexSize) { 
			endChunk = usedSpace;
		}
			
		if(endChunk != 0) {
			begin = (const unsigned char*)(buffer->getBuffer()) + beginChunk;
			end = (const unsigned char*)(buffer->getBuffer()) + endChunk;
			entityCount = entityCount + countEntity(begin, end);

			beginChunk = endChunk;
		}

		//beginChunk = endChunk;
	}

	return entityCount;
}

Status OneConstantStatisticsBuffer::addStatis(unsigned v1, unsigned v2, unsigned v3 /* = 0 */)
{
//	static bool first = true;
	unsigned interVal;
	if ( v1 >= nextHashValue ) {
		interVal = v1;
	} else {
		interVal = v1 - lastId;
	}

	unsigned len;
	if(v1 >= nextHashValue) {
		len = 8;
	} else if(interVal < 16 && v2 <= 8) {
		len = 1;
	} else {
		len = 1 + getLen(interVal - 1) + getLen(v2 - 1);
	}

	if ( isPtrFull(len) == true ) {
		//MessageEngine::showMessage("OneConstantStatisticsBuffer::addStatis()", MessageEngine::INFO);
		usedSpace = writer - (unsigned char*)buffer->getBuffer();
		buffer->resize(STATISTICS_BUFFER_INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
		writer = (unsigned char*)buffer->getBuffer() + usedSpace;
	}

	if ( first || (v1 >= nextHashValue) ) {
		unsigned offset = writer - (uchar*)buffer->getBuffer();
		while (index.size() <= (v1 / ID_HASH)) {
			index.resize(index.size() + 2000, 0);
#ifdef DEBUG
			cout<<"index size"<<index.size()<<" v1 / ID_HASH: "<<(v1 / ID_HASH)<<endl;
#endif
		}

		index[v1 / ID_HASH] = offset;
		while(nextHashValue <= v1) nextHashValue += HASH_RANGE;

		writeUint32(writer, v1);
		writer += 4;
		writeUint32(writer, v2);
		writer += 4;

		first = false;
	} else {
		if(len == 1) {
			*writer = ((v2 - 1) << 4) | (interVal);
			writer++;
		} else {
			*writer = 0x80|((getLen(interVal-1)*5)+getLen(v2 - 1));
			writer++;
			writer = writeDelta0(writer,interVal - 1);
			writer = writeDelta0(writer, v2 - 1);
		}
	}

	lastId = v1;
	usedSpace = writer - (uchar*)buffer->getBuffer();

	return OK;
}

bool OneConstantStatisticsBuffer::find(unsigned value)
{
	//const Triple* l = pos, *r = posLimit;
	int l = 0, r = posLimit - pos;
	int m;
	while (l != r) {
		m = l + ((r - l) / 2);
		if (value > pos[m].value1) {
			l = m + 1;
		} else if ((!m) || value > pos[m - 1].value1) {
			break;
		} else {
			r = m;
		}
	}

	if(l == r)
		return false;
	else {
		pos = &pos[m];
		return true;
	}

}

bool OneConstantStatisticsBuffer::find_last(unsigned value)
{
	//const Triple* l = pos, *r = posLimit;
	int left = 0, right = posLimit - pos;
	int middle = 0;

	while (left < right) {
		middle = left + ((right - left) / 2);
		if (value < pos[middle].value1) {
			right = middle;
		} else if ((!middle) || value < pos[middle + 1].value1) {
			break;
		} else {
			left = middle + 1;
		}
	}

	if(left == right) {
		return false;
	} else {
		pos = &pos[middle];
		return true;
	}
}

Status OneConstantStatisticsBuffer::getStatis(unsigned& v1, unsigned v2 /* = 0 */)
{
	unsigned i;
	unsigned begin = index[ v1 / ID_HASH];
	unsigned end = 0;

	i = v1 / ID_HASH + 1;
	while(i < indexSize) { // get next chunk start offset;
		if(index[i] != 0) {
			end = index[i];
			break;
		}
		i++;
	}

	if(i == indexSize)
		end = usedSpace;

	reader = (unsigned char*)buffer->getBuffer() + begin;

	lastId = readDelta4(reader);
	reader += 4;

	//reader = readId(lastId, reader, true);
	if ( lastId == v1 ) {
		//reader = readId(v1, reader, false);
		v1 = readDelta4(reader);
		return OK;
	}

	const uchar* limit = (uchar*)buffer->getBuffer() + end;
	this->decode(reader - 4, limit);
	if(this->find(v1)) {
		if(pos->value1 == v1) {
			v1 = pos->count;
			return OK;
		}
	}
	v1 = 0;
	return ERROR;
}

Status OneConstantStatisticsBuffer::save(MMapBuffer*& indexBuffer)
{
#ifdef DEBUG
	cout<<"index size: "<<index.size()<<endl;
#endif
	char * writer;
	if(indexBuffer == NULL) {
		indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/statIndex").c_str(), (index.size() + 2) * 4);
		writer = indexBuffer->get_address();
	} else {
		size_t size = indexBuffer->getSize();
		indexBuffer->resize((index.size() + 2) * 4);
		writer = indexBuffer->get_address() + size;
	}
	writer = writeData(writer, usedSpace);
	writer = writeData(writer, index.size());

	vector<unsigned>::iterator iter, limit;

	for(iter = index.begin(), limit = index.end(); iter != limit; iter++) {
		writer = writeData(writer, *iter);
	}
	//memcpy(writer, index, indexSize * sizeof(unsigned));

	return OK;
}

OneConstantStatisticsBuffer* OneConstantStatisticsBuffer::load(StatisticsType type,const string path, char*& indexBuffer)
{
	OneConstantStatisticsBuffer* statBuffer = new OneConstantStatisticsBuffer(path, type);

	unsigned size, first;
	indexBuffer = (char*)readData(indexBuffer, statBuffer->usedSpace);
	indexBuffer = (char*)readData(indexBuffer, size);

	statBuffer->index.resize(0);

	statBuffer->indexSize = size;

	for( unsigned i = 0; i < size; i++ ) {
		indexBuffer = (char*)readData(indexBuffer, first);
		statBuffer->index.push_back(first);
	}

	return statBuffer;
}

Status OneConstantStatisticsBuffer::getIDs(EntityIDBuffer* entBuffer, ID minID, ID maxID)
{
	unsigned i, endEntry;
	unsigned begin = index[ minID / HASH_RANGE], end;
	reader = (uchar*)buffer->getBuffer() + begin;

	i = minID / ID_HASH;
	while(i < indexSize) {
		if(index[i] != 0) {
			end = index[i];
			break;
		}
		i++;
	}
	if(i == indexSize)
		end = usedSpace;
	endEntry = i;

	const uchar* limit = (uchar*)buffer->getBuffer() + end;

	lastId = readDelta4(reader);
	decode(reader, limit);
	if ( lastId != minID ) {
		find(minID);
	}

	i = maxID / ID_HASH + 1;
	unsigned end1;
	while(index[i] == 0 && i < indexSize) {
		i++;
	}
	if(i == indexSize)
		end1 = usedSpace;
	else
		end1 = index[i];

	while(true) {
		if(end == end1) {
			Triple* temp = pos;
			if(find(maxID) == true)
				posLimit = pos + 1;
			pos = temp;
		}

		while(pos < posLimit) {
			entBuffer->insertID(pos->value1);
			pos++;
		}

		begin = end;
		if(begin == end1)
			break;

		endEntry = endEntry + 1;
		while(index[endEntry] != 0 && endEntry < indexSize) {
			endEntry++;
		}
		if(endEntry == indexSize) {
			end = usedSpace;
		} else {
			end = index[endEntry];
		}

		reader = (const unsigned char*)buffer->getBuffer() + begin;
		limit = (const unsigned char*)buffer->getBuffer() + end;
		decode(reader, limit);
	}

	return OK;
}

bool OneConstantStatisticsBuffer::findInsertPoint1(unsigned minID)
{
	int l = 0, r = posLimit - pos, m = 0, len = r - 1;
	while (l <= r) {
		m = (l + r) / 2;
		if (pos[m].value1 < minID) {
			l = m + 1;
		} else if (pos[m].value1 > minID) {
			r = m - 1;
		} else {
			pos = &pos[m];
			return true;
		}
	}

	if(pos[m].value1 > minID){
		while((m > 0) && (pos[--m].value1 > minID));
		if(m >= 0){
			pos = &pos[m+1];
			return true;
		}
	}
	else{
		while((m < len) && (pos[++m].value1 < minID));
		if(m <= len){
			pos = &pos[m];
			return true;
		}
	}

	return false;
}

bool OneConstantStatisticsBuffer::findInsertPoint2(unsigned maxID)
{
	int l = 0, r = posLimit - pos, m = 0, len = r-1;
	while (l <= r) {
		m = (l + r) / 2;
		if (pos[m].value1 < maxID) {
			l = m + 1;
		} else if (pos[m].value1 > maxID) {
			r = m - 1;
		} else {
			posLimit = &pos[m];
			return true;
		}
	}

	if(pos[m].value1 > maxID){
		while((m > 0) && (pos[--m].value1 > maxID));
		if(m >= 0){
			posLimit = &pos[m];
			return true;
		}
	}
	else{
		while((m < len) && (pos[++m].value1 < maxID));
		if(m <= len){
			posLimit = &pos[m-1];
			return true;
		}
	}

	return false;
}

Status OneConstantStatisticsBuffer::getEntityIDs(EntityIDBuffer* entBuffer, ID minID, ID maxID)
{
	if(minID == 0 && maxID == INT_MAX){
		unsigned i = 0, temp = 0, begin = 0, end = 0, tempEnd = 0;
		/*while(i < indexSize){
			if(index[i] != 0){
				begin = index[i];
				break;
			}
			i++;
		}
		temp = i;*/
		begin = index[0];

		i = indexSize - 1;
		while(i >= 0){
			if(index[i] != 0){
				end = index[i];
				break;
			}
			i--;
		}

		const uchar* limit;
		while(begin < end){
			reader = (uchar *)buffer->getBuffer() + begin;
			temp++;
			while(temp <= i){
				if(index[temp] != 0){
					tempEnd = index[temp];
					break;
				}
				temp++;
			}
			limit = (uchar*)buffer->getBuffer() + tempEnd;
			decodeAndInsertID(reader, limit, entBuffer);
			begin = tempEnd;
		}

		limit = (uchar*)buffer->getBuffer() + usedSpace;
		decodeAndInsertID(reader, limit, entBuffer);
	}else{
		unsigned i = minID / ID_HASH, temp1 = 0, temp2 = 0, lastBegin = 0, curBegin = 0, lastEnd = 0, curEnd = 0;
		/*cout<<"minID = "<<minID<<" , maxID = "<<maxID<<" , i = "<<i<<" , indexSize = "<<indexSize<<endl;
		for(size_t k = 0;k < indexSize;){
			cout<<"index["<<k<<"] = "<<index[k]<<" ";
			k++;
			if(!(k % 50))cout<<endl;
		}*/

		//find the first two blocks, note that 'index[0] = 0'
		if(i == 0)lastBegin = index[0];
		else{
			while(i >= 0){
				if(index[i] != 0){
					lastBegin = index[i];
					break;
				}
				i--;
			}
			if(i == 0)lastBegin = index[0];
		}
		if(minID / ID_HASH == indexSize)curBegin = usedSpace;
		else{
			i = minID / ID_HASH + 1;
			while(i < indexSize){
				if(index[i]!=0){
					curBegin = index[i];
					break;
				}
				i++;
			}
			temp1 = i;

			if(i == indexSize)curBegin = usedSpace;
		}

		//find the last two blocks
		i = maxID / ID_HASH;
		while(i < indexSize){
			if(index[i] != 0){
				curEnd = index[i];
				temp2 = i;
				break;
			}
			i++;
		}

		if(i == indexSize){
			curEnd = usedSpace;
			while(--i >= 0){
				if(index[i] != 0){
					lastEnd = index[i];
					temp2 = i;
					break;
				}
			}
		}
		else if(i == maxID / ID_HASH){
			lastEnd = curEnd;
			while(++i < indexSize){
				if(index[i] != 0){
					curEnd = index[i];
					break;
				}
			}
			if(i == indexSize)curEnd = usedSpace;
		}
		else{
			while(--i >= 0){
				if(index[i] != 0){
					lastEnd = index[i];
					temp2 = i;
					break;
				}
			}
		}

		//head second -(relationship)- tail second
		if(curBegin > lastEnd){
			//assert(lastBegin == lastEnd);
			//assert(curBegin == curEnd);

			reader = (uchar*)buffer->getBuffer() + lastBegin;
			const uchar* limit = (uchar*)buffer->getBuffer() + curEnd;
			decodeID(reader,limit);

			bool point1 = findInsertPoint1(minID);
			bool point2 = findInsertPoint2(maxID);
			if(point1 && point2){
				while(pos < posLimit) {
					entBuffer->insertID(pos->value1);
					pos++;
				}
			}
			else return NOT_FOUND;
		}
		else{
			reader = (uchar*)buffer->getBuffer() + lastBegin;
			const uchar* limit = (uchar*)buffer->getBuffer() + curBegin;
			decodeID(reader,limit);
			bool point1 = findInsertPoint1(minID);
			if(point1){
				while(pos < posLimit) {
					entBuffer->insertID(pos->value1);
					pos++;
				}
			}

			unsigned tempEnd = 0;
			while(curBegin < lastEnd){
				reader = (uchar*)buffer->getBuffer() + curBegin;
				while(++temp1 <= temp2){
					if(index[temp1] != 0){
						tempEnd = index[temp1];
						break;
					}
				}

				limit = (uchar*)buffer->getBuffer() + tempEnd;
				decodeAndInsertID(reader,limit,entBuffer);
				curBegin = tempEnd;
			}

			reader = (uchar*)buffer->getBuffer() + lastEnd;
			limit = (uchar*)buffer->getBuffer() + curEnd;
			decodeID(reader,limit);
			bool point2 = findInsertPoint2(maxID);
			if(point2){
				while(pos < posLimit) {
					entBuffer->insertID(pos->value1);
					pos++;
				}
			}
		}
	}

	return OK;
}

void OneConstantStatisticsBuffer::flush(){
	buffer->flush();
}

//////////////////////////////////////////////////////////////////////

TwoConstantStatisticsBuffer::TwoConstantStatisticsBuffer(const string path, StatisticsType type) : StatisticsBuffer(), type(type), reader(NULL)
{
	buffer = new MMapBuffer(path.c_str(), STATISTICS_BUFFER_INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	//index = (Triple*)malloc(MemoryBuffer::pagesize * sizeof(Triple));
	writer = (uchar*)buffer->getBuffer();
	lastId = 0; lastPredicate = 0;
	usedSpace = 0;
	indexPos = 0;
	indexSize = 0; //MemoryBuffer::pagesize;
	index = NULL;
	first = true;
}

TwoConstantStatisticsBuffer::~TwoConstantStatisticsBuffer()
{
	if(buffer != NULL) {
		delete buffer;
	}
	buffer = NULL;
	index = NULL;
}

const uchar* TwoConstantStatisticsBuffer::decode(const uchar* begin, const uchar* end)
{
	unsigned value1 = readDelta4(begin); begin += 4;
	unsigned value2 = readDelta4(begin); begin += 4;
	unsigned count = readDelta4(begin); begin += 4;
	Triple* writer = &triples[0];
	(*writer).value1 = value1;
	(*writer).value2 = value2;
	(*writer).count = count;
	++writer;

	// Decompress the remainder of the page
	while (begin < end) {
		// Decode the header byte
		unsigned info = *(begin++);
		// Small gap only?
		if (info < 0x80) {
			if (!info)
				break;
			count = (info >> 5) + 1;
			value2 += (info & 31);
			(*writer).value1 = value1;
			(*writer).value2 = value2;
			(*writer).count = count;
			++writer;
			continue;
		}
	      // Decode the parts
		switch (info&127) {
		case 0: count=1; break;
		case 1: count=readDelta1(begin)+1; begin+=1; break;
		case 2: count=readDelta2(begin)+1; begin+=2; break;
		case 3: count=readDelta3(begin)+1; begin+=3; break;
		case 4: count=readDelta4(begin)+1; begin+=4; break;
		case 5: value2 += readDelta1(begin); count=1; begin+=1; break;
		case 6: value2 += readDelta1(begin); count=readDelta1(begin+1)+1; begin+=2; break;
		case 7: value2 += readDelta1(begin); count=readDelta2(begin+1)+1; begin+=3; break;
		case 8: value2 += readDelta1(begin); count=readDelta3(begin+1)+1; begin+=4; break;
		case 9: value2 += readDelta1(begin); count=readDelta4(begin+1)+1; begin+=5; break;
		case 10: value2 += readDelta2(begin); count=1; begin+=2; break;
		case 11: value2 += readDelta2(begin); count=readDelta1(begin+2)+1; begin+=3; break;
		case 12: value2 += readDelta2(begin); count=readDelta2(begin+2)+1; begin+=4; break;
		case 13: value2 += readDelta2(begin); count=readDelta3(begin+2)+1; begin+=5; break;
		case 14: value2 += readDelta2(begin); count=readDelta4(begin+2)+1; begin+=6; break;
		case 15: value2 += readDelta3(begin); count=1; begin+=3; break;
		case 16: value2 += readDelta3(begin); count=readDelta1(begin+3)+1; begin+=4; break;
		case 17: value2 += readDelta3(begin); count=readDelta2(begin+3)+1; begin+=5; break;
		case 18: value2 += readDelta3(begin); count=readDelta3(begin+3)+1; begin+=6; break;
		case 19: value2 += readDelta3(begin); count=readDelta4(begin+3)+1; begin+=7; break;
		case 20: value2 += readDelta4(begin); count=1; begin+=4; break;
		case 21: value2 += readDelta4(begin); count=readDelta1(begin+4)+1; begin+=5; break;
		case 22: value2 += readDelta4(begin); count=readDelta2(begin+4)+1; begin+=6; break;
		case 23: value2 += readDelta4(begin); count=readDelta3(begin+4)+1; begin+=7; break;
		case 24: value2 += readDelta4(begin); count=readDelta4(begin+4)+1; begin+=8; break;
		case 25: value1 += readDelta1(begin); value2=0; count=1; begin+=1; break;
		case 26: value1 += readDelta1(begin); value2=0; count=readDelta1(begin+1)+1; begin+=2; break;
		case 27: value1 += readDelta1(begin); value2=0; count=readDelta2(begin+1)+1; begin+=3; break;
		case 28: value1 += readDelta1(begin); value2=0; count=readDelta3(begin+1)+1; begin+=4; break;
		case 29: value1 += readDelta1(begin); value2=0; count=readDelta4(begin+1)+1; begin+=5; break;
		case 30: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=1; begin+=2; break;
		case 31: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=readDelta1(begin+2)+1; begin+=3; break;
		case 32: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=readDelta2(begin+2)+1; begin+=4; break;
		case 33: value1+=readDelta1(begin); value2=readDelta1(begin+1); count=readDelta3(begin+2)+1; begin+=5; break;
		case 34: value1+=readDelta1(begin); value2=readDelta1(begin+1); count=readDelta4(begin+2)+1; begin+=6; break;
		case 35: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=1; begin+=3; break;
		case 36: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta1(begin+3)+1; begin+=4; break;
		case 37: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta2(begin+3)+1; begin+=5; break;
		case 38: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta3(begin+3)+1; begin+=6; break;
		case 39: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta4(begin+3)+1; begin+=7; break;
		case 40: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=1; begin+=4; break;
		case 41: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta1(begin+4)+1; begin+=5; break;
		case 42: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta2(begin+4)+1; begin+=6; break;
		case 43: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta3(begin+4)+1; begin+=7; break;
		case 44: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta4(begin+4)+1; begin+=8; break;
		case 45: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=1; begin+=5; break;
		case 46: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta1(begin+5)+1; begin+=6; break;
		case 47: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta2(begin+5)+1; begin+=7; break;
		case 48: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta3(begin+5)+1; begin+=8; break;
		case 49: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta4(begin+5)+1; begin+=9; break;
		case 50: value1+=readDelta2(begin); value2=0; count=1; begin+=2; break;
		case 51: value1+=readDelta2(begin); value2=0; count=readDelta1(begin+2)+1; begin+=3; break;
		case 52: value1+=readDelta2(begin); value2=0; count=readDelta2(begin+2)+1; begin+=4; break;
		case 53: value1+=readDelta2(begin); value2=0; count=readDelta3(begin+2)+1; begin+=5; break;
		case 54: value1+=readDelta2(begin); value2=0; count=readDelta4(begin+2)+1; begin+=6; break;
		case 55: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=1; begin+=3; break;
		case 56: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta1(begin+3)+1; begin+=4; break;
		case 57: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta2(begin+3)+1; begin+=5; break;
		case 58: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta3(begin+3)+1; begin+=6; break;
		case 59: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta4(begin+3)+1; begin+=7; break;
		case 60: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=1; begin+=4; break;
		case 61: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta1(begin+4)+1; begin+=5; break;
		case 62: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta2(begin+4)+1; begin+=6; break;
		case 63: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta3(begin+4)+1; begin+=7; break;
		case 64: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta4(begin+4)+1; begin+=8; break;
		case 65: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=1; begin+=5; break;
		case 66: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta1(begin+5)+1; begin+=6; break;
		case 67: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta2(begin+5)+1; begin+=7; break;
		case 68: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta3(begin+5)+1; begin+=8; break;
		case 69: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta4(begin+5)+1; begin+=9; break;
		case 70: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=1; begin+=6; break;
		case 71: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta1(begin+6)+1; begin+=7; break;
		case 72: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta2(begin+6)+1; begin+=8; break;
		case 73: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta3(begin+6)+1; begin+=9; break;
		case 74: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta4(begin+6)+1; begin+=10; break;
		case 75: value1+=readDelta3(begin); value2=0; count=1; begin+=3; break;
		case 76: value1+=readDelta3(begin); value2=0; count=readDelta1(begin+3)+1; begin+=4; break;
		case 77: value1+=readDelta3(begin); value2=0; count=readDelta2(begin+3)+1; begin+=5; break;
		case 78: value1+=readDelta3(begin); value2=0; count=readDelta3(begin+3)+1; begin+=6; break;
		case 79: value1+=readDelta3(begin); value2=0; count=readDelta4(begin+3)+1; begin+=7; break;
		case 80: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=1; begin+=4; break;
		case 81: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta1(begin+4)+1; begin+=5; break;
		case 82: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta2(begin+4)+1; begin+=6; break;
		case 83: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta3(begin+4)+1; begin+=7; break;
		case 84: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta4(begin+4)+1; begin+=8; break;
		case 85: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=1; begin+=5; break;
		case 86: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta1(begin+5)+1; begin+=6; break;
		case 87: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta2(begin+5)+1; begin+=7; break;
		case 88: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta3(begin+5)+1; begin+=8; break;
		case 89: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta4(begin+5)+1; begin+=9; break;
		case 90: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=1; begin+=6; break;
		case 91: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta1(begin+6)+1; begin+=7; break;
		case 92: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta2(begin+6)+1; begin+=8; break;
		case 93: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta3(begin+6)+1; begin+=9; break;
		case 94: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta4(begin+6)+1; begin+=10; break;
		case 95: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=1; begin+=7; break;
		case 96: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta1(begin+7)+1; begin+=8; break;
		case 97: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta2(begin+7)+1; begin+=9; break;
		case 98: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta3(begin+7)+1; begin+=10; break;
		case 99: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta4(begin+7)+1; begin+=11; break;
		case 100: value1+=readDelta4(begin); value2=0; count=1; begin+=4; break;
		case 101: value1+=readDelta4(begin); value2=0; count=readDelta1(begin+4)+1; begin+=5; break;
		case 102: value1+=readDelta4(begin); value2=0; count=readDelta2(begin+4)+1; begin+=6; break;
		case 103: value1+=readDelta4(begin); value2=0; count=readDelta3(begin+4)+1; begin+=7; break;
		case 104: value1+=readDelta4(begin); value2=0; count=readDelta4(begin+4)+1; begin+=8; break;
		case 105: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=1; begin+=5; break;
		case 106: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta1(begin+5)+1; begin+=6; break;
		case 107: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta2(begin+5)+1; begin+=7; break;
		case 108: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta3(begin+5)+1; begin+=8; break;
		case 109: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta4(begin+5)+1; begin+=9; break;
		case 110: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=1; begin+=6; break;
		case 111: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta1(begin+6)+1; begin+=7; break;
		case 112: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta2(begin+6)+1; begin+=8; break;
		case 113: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta3(begin+6)+1; begin+=9; break;
		case 114: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta4(begin+6)+1; begin+=10; break;
		case 115: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=1; begin+=7; break;
		case 116: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta1(begin+7)+1; begin+=8; break;
		case 117: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta2(begin+7)+1; begin+=9; break;
		case 118: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta3(begin+7)+1; begin+=10; break;
		case 119: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta4(begin+7)+1; begin+=11; break;
		case 120: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=1; begin+=8; break;
		case 121: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta1(begin+8)+1; begin+=9; break;
		case 122: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta2(begin+8)+1; begin+=10; break;
		case 123: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta3(begin+8)+1; begin+=11; break;
		case 124: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta4(begin+8)+1; begin+=12; break;
		}
		(*writer).value1=value1;
		(*writer).value2=value2;
		(*writer).count=count;
		++writer;
	}

	// Update the entries
	pos=triples;
	posLimit=writer;

	return begin;
}

const uchar* TwoConstantStatisticsBuffer::decodeIdAndPredicate(const uchar* begin, const uchar* end)
{
	unsigned value1 = readDelta4(begin); begin += 4;
	unsigned value2 = readDelta4(begin); begin += 4;
	unsigned count = readDelta4(begin); begin += 4;
	Triple* writer = &triples[0];
	(*writer).value1 = value1;
	(*writer).value2 = value2;
	(*writer).count = count;
	++writer;

	// Decompress the remainder of the page
	while (begin < end) {
		// Decode the header byte
		unsigned info = *(begin++);
		// Small gap only?
		if (info < 0x80) {
			if (!info)
				break;
			//count = (info >> 5) + 1;
			value2 += (info & 31);
			(*writer).value1 = value1;
			(*writer).value2 = value2;
			(*writer).count = count;
			++writer;
			continue;
		}
	      // Decode the parts
		switch (info&127) {
		case 0: break;
		case 1: begin+=1; break;
		case 2: begin+=2; break;
		case 3: begin+=3; break;
		case 4: begin+=4; break;
		case 5: value2 += readDelta1(begin); begin+=1; break;
		case 6: value2 += readDelta1(begin); begin+=2; break;
		case 7: value2 += readDelta1(begin); begin+=3; break;
		case 8: value2 += readDelta1(begin); begin+=4; break;
		case 9: value2 += readDelta1(begin); begin+=5; break;
		case 10: value2 += readDelta2(begin); begin+=2; break;
		case 11: value2 += readDelta2(begin); begin+=3; break;
		case 12: value2 += readDelta2(begin); begin+=4; break;
		case 13: value2 += readDelta2(begin); begin+=5; break;
		case 14: value2 += readDelta2(begin); begin+=6; break;
		case 15: value2 += readDelta3(begin); begin+=3; break;
		case 16: value2 += readDelta3(begin); begin+=4; break;
		case 17: value2 += readDelta3(begin); begin+=5; break;
		case 18: value2 += readDelta3(begin); begin+=6; break;
		case 19: value2 += readDelta3(begin); begin+=7; break;
		case 20: value2 += readDelta4(begin); begin+=4; break;
		case 21: value2 += readDelta4(begin); begin+=5; break;
		case 22: value2 += readDelta4(begin); begin+=6; break;
		case 23: value2 += readDelta4(begin); begin+=7; break;
		case 24: value2 += readDelta4(begin); begin+=8; break;
		case 25: value1 += readDelta1(begin); value2=0; begin+=1; break;
		case 26: value1 += readDelta1(begin); value2=0; begin+=2; break;
		case 27: value1 += readDelta1(begin); value2=0; begin+=3; break;
		case 28: value1 += readDelta1(begin); value2=0; begin+=4; break;
		case 29: value1 += readDelta1(begin); value2=0; begin+=5; break;
		case 30: value1 += readDelta1(begin); value2=readDelta1(begin+1); begin+=2; break;
		case 31: value1 += readDelta1(begin); value2=readDelta1(begin+1); begin+=3; break;
		case 32: value1 += readDelta1(begin); value2=readDelta1(begin+1); begin+=4; break;
		case 33: value1+=readDelta1(begin); value2=readDelta1(begin+1); begin+=5; break;
		case 34: value1+=readDelta1(begin); value2=readDelta1(begin+1); begin+=6; break;
		case 35: value1+=readDelta1(begin); value2=readDelta2(begin+1); begin+=3; break;
		case 36: value1+=readDelta1(begin); value2=readDelta2(begin+1); begin+=4; break;
		case 37: value1+=readDelta1(begin); value2=readDelta2(begin+1); begin+=5; break;
		case 38: value1+=readDelta1(begin); value2=readDelta2(begin+1); begin+=6; break;
		case 39: value1+=readDelta1(begin); value2=readDelta2(begin+1); begin+=7; break;
		case 40: value1+=readDelta1(begin); value2=readDelta3(begin+1); begin+=4; break;
		case 41: value1+=readDelta1(begin); value2=readDelta3(begin+1); begin+=5; break;
		case 42: value1+=readDelta1(begin); value2=readDelta3(begin+1); begin+=6; break;
		case 43: value1+=readDelta1(begin); value2=readDelta3(begin+1); begin+=7; break;
		case 44: value1+=readDelta1(begin); value2=readDelta3(begin+1); begin+=8; break;
		case 45: value1+=readDelta1(begin); value2=readDelta4(begin+1); begin+=5; break;
		case 46: value1+=readDelta1(begin); value2=readDelta4(begin+1); begin+=6; break;
		case 47: value1+=readDelta1(begin); value2=readDelta4(begin+1); begin+=7; break;
		case 48: value1+=readDelta1(begin); value2=readDelta4(begin+1); begin+=8; break;
		case 49: value1+=readDelta1(begin); value2=readDelta4(begin+1); begin+=9; break;
		case 50: value1+=readDelta2(begin); value2=0; begin+=2; break;
		case 51: value1+=readDelta2(begin); value2=0; begin+=3; break;
		case 52: value1+=readDelta2(begin); value2=0; begin+=4; break;
		case 53: value1+=readDelta2(begin); value2=0; begin+=5; break;
		case 54: value1+=readDelta2(begin); value2=0; begin+=6; break;
		case 55: value1+=readDelta2(begin); value2=readDelta1(begin+2); begin+=3; break;
		case 56: value1+=readDelta2(begin); value2=readDelta1(begin+2); begin+=4; break;
		case 57: value1+=readDelta2(begin); value2=readDelta1(begin+2); begin+=5; break;
		case 58: value1+=readDelta2(begin); value2=readDelta1(begin+2); begin+=6; break;
		case 59: value1+=readDelta2(begin); value2=readDelta1(begin+2); begin+=7; break;
		case 60: value1+=readDelta2(begin); value2=readDelta2(begin+2); begin+=4; break;
		case 61: value1+=readDelta2(begin); value2=readDelta2(begin+2); begin+=5; break;
		case 62: value1+=readDelta2(begin); value2=readDelta2(begin+2); begin+=6; break;
		case 63: value1+=readDelta2(begin); value2=readDelta2(begin+2); begin+=7; break;
		case 64: value1+=readDelta2(begin); value2=readDelta2(begin+2); begin+=8; break;
		case 65: value1+=readDelta2(begin); value2=readDelta3(begin+2); begin+=5; break;
		case 66: value1+=readDelta2(begin); value2=readDelta3(begin+2); begin+=6; break;
		case 67: value1+=readDelta2(begin); value2=readDelta3(begin+2); begin+=7; break;
		case 68: value1+=readDelta2(begin); value2=readDelta3(begin+2); begin+=8; break;
		case 69: value1+=readDelta2(begin); value2=readDelta3(begin+2); begin+=9; break;
		case 70: value1+=readDelta2(begin); value2=readDelta4(begin+2); begin+=6; break;
		case 71: value1+=readDelta2(begin); value2=readDelta4(begin+2); begin+=7; break;
		case 72: value1+=readDelta2(begin); value2=readDelta4(begin+2); begin+=8; break;
		case 73: value1+=readDelta2(begin); value2=readDelta4(begin+2); begin+=9; break;
		case 74: value1+=readDelta2(begin); value2=readDelta4(begin+2); begin+=10; break;
		case 75: value1+=readDelta3(begin); value2=0; begin+=3; break;
		case 76: value1+=readDelta3(begin); value2=0; begin+=4; break;
		case 77: value1+=readDelta3(begin); value2=0; begin+=5; break;
		case 78: value1+=readDelta3(begin); value2=0; begin+=6; break;
		case 79: value1+=readDelta3(begin); value2=0; begin+=7; break;
		case 80: value1+=readDelta3(begin); value2=readDelta1(begin+3); begin+=4; break;
		case 81: value1+=readDelta3(begin); value2=readDelta1(begin+3); begin+=5; break;
		case 82: value1+=readDelta3(begin); value2=readDelta1(begin+3); begin+=6; break;
		case 83: value1+=readDelta3(begin); value2=readDelta1(begin+3); begin+=7; break;
		case 84: value1+=readDelta3(begin); value2=readDelta1(begin+3); begin+=8; break;
		case 85: value1+=readDelta3(begin); value2=readDelta2(begin+3); begin+=5; break;
		case 86: value1+=readDelta3(begin); value2=readDelta2(begin+3); begin+=6; break;
		case 87: value1+=readDelta3(begin); value2=readDelta2(begin+3); begin+=7; break;
		case 88: value1+=readDelta3(begin); value2=readDelta2(begin+3); begin+=8; break;
		case 89: value1+=readDelta3(begin); value2=readDelta2(begin+3); begin+=9; break;
		case 90: value1+=readDelta3(begin); value2=readDelta3(begin+3); begin+=6; break;
		case 91: value1+=readDelta3(begin); value2=readDelta3(begin+3); begin+=7; break;
		case 92: value1+=readDelta3(begin); value2=readDelta3(begin+3); begin+=8; break;
		case 93: value1+=readDelta3(begin); value2=readDelta3(begin+3); begin+=9; break;
		case 94: value1+=readDelta3(begin); value2=readDelta3(begin+3); begin+=10; break;
		case 95: value1+=readDelta3(begin); value2=readDelta4(begin+3); begin+=7; break;
		case 96: value1+=readDelta3(begin); value2=readDelta4(begin+3); begin+=8; break;
		case 97: value1+=readDelta3(begin); value2=readDelta4(begin+3); begin+=9; break;
		case 98: value1+=readDelta3(begin); value2=readDelta4(begin+3); begin+=10; break;
		case 99: value1+=readDelta3(begin); value2=readDelta4(begin+3); begin+=11; break;
		case 100: value1+=readDelta4(begin); value2=0; begin+=4; break;
		case 101: value1+=readDelta4(begin); value2=0; begin+=5; break;
		case 102: value1+=readDelta4(begin); value2=0; begin+=6; break;
		case 103: value1+=readDelta4(begin); value2=0; begin+=7; break;
		case 104: value1+=readDelta4(begin); value2=0; begin+=8; break;
		case 105: value1+=readDelta4(begin); value2=readDelta1(begin+4); begin+=5; break;
		case 106: value1+=readDelta4(begin); value2=readDelta1(begin+4); begin+=6; break;
		case 107: value1+=readDelta4(begin); value2=readDelta1(begin+4); begin+=7; break;
		case 108: value1+=readDelta4(begin); value2=readDelta1(begin+4); begin+=8; break;
		case 109: value1+=readDelta4(begin); value2=readDelta1(begin+4); begin+=9; break;
		case 110: value1+=readDelta4(begin); value2=readDelta2(begin+4); begin+=6; break;
		case 111: value1+=readDelta4(begin); value2=readDelta2(begin+4); begin+=7; break;
		case 112: value1+=readDelta4(begin); value2=readDelta2(begin+4); begin+=8; break;
		case 113: value1+=readDelta4(begin); value2=readDelta2(begin+4); begin+=9; break;
		case 114: value1+=readDelta4(begin); value2=readDelta2(begin+4); begin+=10; break;
		case 115: value1+=readDelta4(begin); value2=readDelta3(begin+4); begin+=7; break;
		case 116: value1+=readDelta4(begin); value2=readDelta3(begin+4); begin+=8; break;
		case 117: value1+=readDelta4(begin); value2=readDelta3(begin+4); begin+=9; break;
		case 118: value1+=readDelta4(begin); value2=readDelta3(begin+4); begin+=10; break;
		case 119: value1+=readDelta4(begin); value2=readDelta3(begin+4); begin+=11; break;
		case 120: value1+=readDelta4(begin); value2=readDelta4(begin+4); begin+=8; break;
		case 121: value1+=readDelta4(begin); value2=readDelta4(begin+4); begin+=9; break;
		case 122: value1+=readDelta4(begin); value2=readDelta4(begin+4); begin+=10; break;
		case 123: value1+=readDelta4(begin); value2=readDelta4(begin+4); begin+=11; break;
		case 124: value1+=readDelta4(begin); value2=readDelta4(begin+4); begin+=12; break;
		}
		(*writer).value1=value1;
		(*writer).value2=value2;
		(*writer).count=count;
		++writer;
	}

	// Update the entries
	pos=triples;
	posLimit=writer;

	return begin;
}

static inline bool greater(unsigned a1,unsigned a2,unsigned b1,unsigned b2) {
   return (a1>b1)||((a1==b1)&&(a2>b2));
}

static inline bool less(unsigned a1, unsigned a2, unsigned b1, unsigned b2) {
	return (a1 < b1) || ((a1 == b1) && (a2 < b2));
}
/*
 * find the first entry >= (value1, value2);
 * pos: the start address of the first triple;
 * posLimit: the end address of last triple;
 */
bool TwoConstantStatisticsBuffer::find(unsigned value1, unsigned value2)
{
	//const Triple* l = pos, *r = posLimit;
	int left = 0, right = posLimit - pos;
	int middle;

	while (left != right) {
		middle = left + ((right - left) / 2);
		if (::greater(value1, value2, pos[middle].value1, pos[middle].value2)) {
			left = middle + 1;
		} else if ((!middle) || ::greater(value1, value2, pos[middle - 1].value1, pos[middle -1].value2)) {
			break;
		} else {
			right = middle;
		}
	}

	if(left == right) {
		return false;
	} else {
		pos = &pos[middle];
		return true;
	}
}

/*
 * find the last entry <= (value1, value2);
 * pos: the start address of the first triple;
 * posLimit: the end address of last triple;
 */
bool TwoConstantStatisticsBuffer::find_last(unsigned value1, unsigned value2)
{
	//const Triple* l = pos, *r = posLimit;
	int left = 0, right = posLimit - pos;
	int middle = 0;

	while (left < right) {
		middle = left + ((right - left) / 2);
		if (::less(value1, value2, pos[middle].value1, pos[middle].value2)) {
			right = middle;
		} else if ((!middle) || ::less(value1, value2, pos[middle + 1].value1, pos[middle + 1].value2)) {
			break;
		} else {
			left = middle + 1;
		}
	}

	if(left == right) {
		return false;
	} else {
		pos = &pos[middle];
		return true;
	}
}

int TwoConstantStatisticsBuffer::findPredicate(unsigned value1,Triple*pos,Triple* posLimit){
	int low = 0, high= posLimit - pos,mid;
	while (low <= high) {
		mid = low + ((high - low)/2);
		if (pos[mid].value1 == value1)
			return mid;
		if (pos[mid].value1 > value1)
			high = mid - 1;
		else
			low = mid + 1;
	}
	return -1;

}

Status TwoConstantStatisticsBuffer::getStatis(unsigned& v1, unsigned v2)
{
	pos = index, posLimit = index + indexPos;
	find(v1, v2);
	if(::greater(pos->value1, pos->value2, v1, v2))
		pos--;

	unsigned start = pos->count; pos++;
	unsigned end = pos->count;
	if(pos == (index + indexPos))
		end = usedSpace;

	const unsigned char* begin = (uchar*)buffer->getBuffer() + start, *limit = (uchar*)buffer->getBuffer() + end;
	decode(begin, limit);
	find(v1, v2);
	if(pos->value1 == v1 && pos->value2 == v2) {
		v1 = pos->count;
		return OK;
	}
	v1 = 0;
	return NOT_FOUND;
}

Status TwoConstantStatisticsBuffer::addStatis(unsigned v1, unsigned v2, unsigned v3)
{
//	static bool first = true;
	unsigned len = 0;

	// otherwise store the compressed value
	if ( v1 == lastId && (v2 - lastPredicate) < 32 && v3 < 5) {
		len = 1;
	} else if(v1 == lastId) {
		len = 1 + getLen(v2 - lastPredicate) + getLen(v3 - 1);
	} else {
		len = 1+ getLen(v1-lastId)+ getLen(v2)+getLen(v3 - 1);
	}

	if ( first == true || ( usedSpace + len ) > buffer->getSize() ) {
		usedSpace = writer - (uchar*)buffer->getBuffer();
		buffer->resize(STATISTICS_BUFFER_INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
		writer = (uchar*)buffer->getBuffer() + usedSpace;

		writeUint32(writer, v1); writer += 4;
		writeUint32(writer, v2); writer += 4;
		writeUint32(writer, v3); writer += 4;

		if((indexPos + 1) >= indexSize) {
#ifdef DEBUF
			cout<<"indexPos: "<<indexPos<<" indexSize: "<<indexSize<<endl;
#endif
			index = (Triple*)realloc(index, indexSize * sizeof(Triple) + MemoryBuffer::pagesize * sizeof(Triple));
			indexSize += MemoryBuffer::pagesize;
		}

		index[indexPos].value1 = v1;
		index[indexPos].value2 = v2;
		index[indexPos].count = usedSpace; //record offset
		indexPos++;

		first = false;
	} else {
		if (v1 == lastId && v2 - lastPredicate < 32 && v3 < 5) {
			*writer++ = ((v3 - 1) << 5) | (v2 - lastPredicate);
		} else if (v1 == lastId) {
			*writer++ = 0x80 | (getLen(v1 - lastId) * 25 + getLen(v2 - lastPredicate) * 5 + getLen(v3 - 1));
			writer = writeDelta0(writer, v2 - lastPredicate);
			writer = writeDelta0(writer, v3 - 1);
		} else {
			*writer++ = 0x80 | (getLen(v1 - lastId) * 25 + getLen(v2) * 5 + getLen(v3 - 1));
			writer = writeDelta0(writer, v1 - lastId);
			writer = writeDelta0(writer, v2);
			writer = writeDelta0(writer, v3 - 1);
		}
	}

	lastId = v1; lastPredicate = v2;

	usedSpace = writer - (uchar*)buffer->getBuffer();
	return OK;
}

Status TwoConstantStatisticsBuffer::save(MMapBuffer*& indexBuffer)
{
	char* writer;
	if(indexBuffer == NULL) {
		indexBuffer = MMapBuffer::create(string(string(DATABASE_PATH) + "/statIndex").c_str(), indexPos * sizeof(Triple) + 2 * sizeof(unsigned));
		writer = indexBuffer->get_address();
	} else {
		size_t size = indexBuffer->getSize();
		indexBuffer->resize(indexPos * sizeof(Triple) + 2 * sizeof(unsigned));
		writer = indexBuffer->get_address() + size;
	}

	writer = writeData(writer, usedSpace);
	writer = writeData(writer, indexPos);

	memcpy(writer, (char*)index, indexPos * sizeof(Triple));
#ifdef DEBUG
	for(int i = 0; i < 3; i++)
	{
		cout<<index[i].value1<<" : "<<index[i].value2<<" : "<<index[i].count<<endl;
	}

	cout<<"indexPos: "<<indexPos<<endl;
#endif
	free(index);

	return OK;
}

TwoConstantStatisticsBuffer* TwoConstantStatisticsBuffer::load(StatisticsType type, const string path, char*& indexBuffer)
{
	TwoConstantStatisticsBuffer* statBuffer = new TwoConstantStatisticsBuffer(path, type);

	indexBuffer = (char*)readData(indexBuffer, statBuffer->usedSpace);
	indexBuffer = (char*)readData(indexBuffer, statBuffer->indexPos);
#ifdef DEBUG
	cout<<__FUNCTION__<<"indexPos: "<<statBuffer->indexPos<<endl;
#endif
	// load index;
	statBuffer->index = (Triple*)indexBuffer;
	indexBuffer = indexBuffer + statBuffer->indexPos * sizeof(Triple);

#ifdef DEBUG
	for(int i = 0; i < 3; i++)
	{
		cout<<statBuffer->index[i].value1<<" : "<<statBuffer->index[i].value2<<" : "<<statBuffer->index[i].count<<endl;
	}
#endif

	return statBuffer;
}

unsigned TwoConstantStatisticsBuffer::getLen(unsigned v)
{
	if (v>=(1<<24))
		return 4; 
	else if (v>=(1<<16))
		return 3;
	else if (v>=(1<<8)) 
		return 2;
	else if(v > 0)
		return 1;
	else
		return 0;
}

Status TwoConstantStatisticsBuffer::getPredicatesByID(unsigned id,EntityIDBuffer* entBuffer, ID minID, ID maxID) {
	Triple* pos, *posLimit;
	pos = index;
	posLimit = index + indexPos;
	//cout<<"indexPos = "<<indexPos<<" , usedSpace = "<<usedSpace<<" , pos = "<<pos<<" , posLimit = "<<posLimit<<endl;
	//find(id, pos, posLimit);
	findID(id, pos, posLimit);
	//cout<<"pos = "<<pos<<" , posLimit = "<<posLimit<<endl;
	assert(pos >= index && pos < posLimit);
	Triple* startChunk = pos;
	Triple* endChunk = pos;
	while (startChunk->value1 > id && startChunk > index) {
		startChunk--;
	}
	while (endChunk->value1 <= id && endChunk < posLimit) {
		endChunk++;
	}

	const unsigned char* begin, *limit;
	Triple* chunkIter = startChunk;
	while (chunkIter < endChunk) {
		begin = (uchar*) (buffer->get_address() + chunkIter->count);
		//printf("1: %p  %p  %u\n",begin, buffer->get_address() ,chunkIter->count);
		chunkIter++;
		if (chunkIter == index + indexPos)
			limit = (uchar*) (buffer->get_address() + usedSpace);
		else
			limit = (uchar*) (buffer->get_address() + chunkIter->count);
		//printf("2: %p  %u\n",limit ,chunkIter->count);

		//Triple* triples = (Triple *)(new unsigned[3 * MemoryBuffer::pagesize]);
		Triple* triples = (Triple *)malloc(3 * sizeof(Triple) * MemoryBuffer::pagesize);
		decode(begin, limit, triples, pos, posLimit);

		int mid = findPredicate(id, pos, posLimit), loc = mid;
		//cout << mid << "  " << loc << endl;


		if (loc == -1)
			continue;
		if((pos[loc].value2 >= minID) && (pos[loc].value2 <= maxID))entBuffer->insertID(pos[loc].value2);
		//cout << "result:" << pos[loc].value2<< endl;
		while ((loc > 0) && (pos[--loc].value1 == id)) {
			if((pos[loc].value2 >= minID) && (pos[loc].value2 <= maxID))entBuffer->insertID(pos[loc].value2);
			//cout << "result:" << pos[loc].value2<< endl;
		}
		loc = mid;
		while ((loc < posLimit - pos) && (pos[++loc].value1 == id)) {
			if((pos[loc].value2 >= minID) && (pos[loc].value2 <= maxID))entBuffer->insertID(pos[loc].value2);
			//cout << "result:" << pos[loc].value2<< endl;
		}
		//delete triples;
		free(triples);
	}

	return OK;
}

bool TwoConstantStatisticsBuffer::find(unsigned value1,Triple*& pos,Triple*& posLimit)
{//find by the value1
	int left = 0, right = posLimit - pos;
	int middle=0;

	while (left < right) {
		middle = left + ((right - left) / 2);
		if (value1 > pos[middle].value1) {
			left = middle +1;
		} else if ((!middle) || value1 > pos[middle - 1].value1) {
			break;
		} else {
			right = middle;
		}
	}

	if(left == right) {
		pos = &pos[middle];
		return false;
	} else {
		pos = &pos[middle];
		return true;
	}
}

bool TwoConstantStatisticsBuffer::findID(unsigned value1,Triple*& pos,Triple*& posLimit)
{//find by the value1
	int left = 0, right = posLimit - pos;
	int middle=0;
	bool hit = false;

	while (left < right) {
		middle = (left + right) / 2;
		if (value1 > pos[middle].value1) {
			left = middle+1;
		} else if (value1 < pos[middle].value1) {
			right = middle-1;
		} else {
			hit = true;
			break;
		}
	}

	if(hit){
		while(middle && (value1 == pos[--middle].value1));
		pos = &pos[middle];
		return true;
	}else{
		while(middle && (value1 < pos[middle].value1))middle--;
		pos = &pos[middle];
		return false;
	}
}

const uchar* TwoConstantStatisticsBuffer::decode(const uchar* begin, const uchar* end,Triple*triples,Triple* &pos,Triple* &posLimit)
{
	unsigned value1 = readDelta4(begin); begin += 4;
	unsigned value2 = readDelta4(begin); begin += 4;
	unsigned count = readDelta4(begin); begin += 4;
	Triple* writer = &triples[0];
	(*writer).value1 = value1;
	(*writer).value2 = value2;
	(*writer).count = count;
	//cout << "value1:" << value1 << "   value2:" << value2 << "   count:" << count<<endl;
	++writer;

	// Decompress the remainder of the page
	while (begin < end) {
		// Decode the header byte
		unsigned info = *(begin++);
		// Small gap only?
		if (info < 0x80) {
			if (!info){
				break;
			}
			count = (info >> 5) + 1;
			value2 += (info & 31);
			(*writer).value1 = value1;
			(*writer).value2 = value2;
			(*writer).count = count;
//			cout << "value1:" << value1 << "   value2:" << value2 << "   count:" << count<<endl;
			++writer;
			continue;
		}
	      // Decode the parts
 		switch (info&127) {
		case 0: count=1; break;
		case 1: count=readDelta1(begin)+1; begin+=1; break;
		case 2: count=readDelta2(begin)+1; begin+=2; break;
		case 3: count=readDelta3(begin)+1; begin+=3; break;
		case 4: count=readDelta4(begin)+1; begin+=4; break;
		case 5: value2 += readDelta1(begin); count=1; begin+=1; break;
		case 6: value2 += readDelta1(begin); count=readDelta1(begin+1)+1; begin+=2; break;
		case 7: value2 += readDelta1(begin); count=readDelta2(begin+1)+1; begin+=3; break;
		case 8: value2 += readDelta1(begin); count=readDelta3(begin+1)+1; begin+=4; break;
		case 9: value2 += readDelta1(begin); count=readDelta4(begin+1)+1; begin+=5; break;
		case 10: value2 += readDelta2(begin); count=1; begin+=2; break;
		case 11: value2 += readDelta2(begin); count=readDelta1(begin+2)+1; begin+=3; break;
		case 12: value2 += readDelta2(begin); count=readDelta2(begin+2)+1; begin+=4; break;
		case 13: value2 += readDelta2(begin); count=readDelta3(begin+2)+1; begin+=5; break;
		case 14: value2 += readDelta2(begin); count=readDelta4(begin+2)+1; begin+=6; break;
		case 15: value2 += readDelta3(begin); count=1; begin+=3; break;
		case 16: value2 += readDelta3(begin); count=readDelta1(begin+3)+1; begin+=4; break;
		case 17: value2 += readDelta3(begin); count=readDelta2(begin+3)+1; begin+=5; break;
		case 18: value2 += readDelta3(begin); count=readDelta3(begin+3)+1; begin+=6; break;
		case 19: value2 += readDelta3(begin); count=readDelta4(begin+3)+1; begin+=7; break;
		case 20: value2 += readDelta4(begin); count=1; begin+=4; break;
		case 21: value2 += readDelta4(begin); count=readDelta1(begin+4)+1; begin+=5; break;
		case 22: value2 += readDelta4(begin); count=readDelta2(begin+4)+1; begin+=6; break;
		case 23: value2 += readDelta4(begin); count=readDelta3(begin+4)+1; begin+=7; break;
		case 24: value2 += readDelta4(begin); count=readDelta4(begin+4)+1; begin+=8; break;
		case 25: value1 += readDelta1(begin); value2=0; count=1; begin+=1; break;
		case 26: value1 += readDelta1(begin); value2=0; count=readDelta1(begin+1)+1; begin+=2; break;
		case 27: value1 += readDelta1(begin); value2=0; count=readDelta2(begin+1)+1; begin+=3; break;
		case 28: value1 += readDelta1(begin); value2=0; count=readDelta3(begin+1)+1; begin+=4; break;
		case 29: value1 += readDelta1(begin); value2=0; count=readDelta4(begin+1)+1; begin+=5; break;
		case 30: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=1; begin+=2; break;
		case 31: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=readDelta1(begin+2)+1; begin+=3; break;
		case 32: value1 += readDelta1(begin); value2=readDelta1(begin+1); count=readDelta2(begin+2)+1; begin+=4; break;
		case 33: value1+=readDelta1(begin); value2=readDelta1(begin+1); count=readDelta3(begin+2)+1; begin+=5; break;
		case 34: value1+=readDelta1(begin); value2=readDelta1(begin+1); count=readDelta4(begin+2)+1; begin+=6; break;
		case 35: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=1; begin+=3; break;
		case 36: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta1(begin+3)+1; begin+=4; break;
		case 37: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta2(begin+3)+1; begin+=5; break;
		case 38: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta3(begin+3)+1; begin+=6; break;
		case 39: value1+=readDelta1(begin); value2=readDelta2(begin+1); count=readDelta4(begin+3)+1; begin+=7; break;
		case 40: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=1; begin+=4; break;
		case 41: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta1(begin+4)+1; begin+=5; break;
		case 42: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta2(begin+4)+1; begin+=6; break;
		case 43: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta3(begin+4)+1; begin+=7; break;
		case 44: value1+=readDelta1(begin); value2=readDelta3(begin+1); count=readDelta4(begin+4)+1; begin+=8; break;
		case 45: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=1; begin+=5; break;
		case 46: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta1(begin+5)+1; begin+=6; break;
		case 47: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta2(begin+5)+1; begin+=7; break;
		case 48: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta3(begin+5)+1; begin+=8; break;
		case 49: value1+=readDelta1(begin); value2=readDelta4(begin+1); count=readDelta4(begin+5)+1; begin+=9; break;
		case 50: value1+=readDelta2(begin); value2=0; count=1; begin+=2; break;
		case 51: value1+=readDelta2(begin); value2=0; count=readDelta1(begin+2)+1; begin+=3; break;
		case 52: value1+=readDelta2(begin); value2=0; count=readDelta2(begin+2)+1; begin+=4; break;
		case 53: value1+=readDelta2(begin); value2=0; count=readDelta3(begin+2)+1; begin+=5; break;
		case 54: value1+=readDelta2(begin); value2=0; count=readDelta4(begin+2)+1; begin+=6; break;
		case 55: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=1; begin+=3; break;
		case 56: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta1(begin+3)+1; begin+=4; break;
		case 57: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta2(begin+3)+1; begin+=5; break;
		case 58: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta3(begin+3)+1; begin+=6; break;
		case 59: value1+=readDelta2(begin); value2=readDelta1(begin+2); count=readDelta4(begin+3)+1; begin+=7; break;
		case 60: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=1; begin+=4; break;
		case 61: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta1(begin+4)+1; begin+=5; break;
		case 62: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta2(begin+4)+1; begin+=6; break;
		case 63: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta3(begin+4)+1; begin+=7; break;
		case 64: value1+=readDelta2(begin); value2=readDelta2(begin+2); count=readDelta4(begin+4)+1; begin+=8; break;
		case 65: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=1; begin+=5; break;
		case 66: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta1(begin+5)+1; begin+=6; break;
		case 67: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta2(begin+5)+1; begin+=7; break;
		case 68: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta3(begin+5)+1; begin+=8; break;
		case 69: value1+=readDelta2(begin); value2=readDelta3(begin+2); count=readDelta4(begin+5)+1; begin+=9; break;
		case 70: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=1; begin+=6; break;
		case 71: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta1(begin+6)+1; begin+=7; break;
		case 72: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta2(begin+6)+1; begin+=8; break;
		case 73: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta3(begin+6)+1; begin+=9; break;
		case 74: value1+=readDelta2(begin); value2=readDelta4(begin+2); count=readDelta4(begin+6)+1; begin+=10; break;
		case 75: value1+=readDelta3(begin); value2=0; count=1; begin+=3; break;
		case 76: value1+=readDelta3(begin); value2=0; count=readDelta1(begin+3)+1; begin+=4; break;
		case 77: value1+=readDelta3(begin); value2=0; count=readDelta2(begin+3)+1; begin+=5; break;
		case 78: value1+=readDelta3(begin); value2=0; count=readDelta3(begin+3)+1; begin+=6; break;
		case 79: value1+=readDelta3(begin); value2=0; count=readDelta4(begin+3)+1; begin+=7; break;
		case 80: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=1; begin+=4; break;
		case 81: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta1(begin+4)+1; begin+=5; break;
		case 82: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta2(begin+4)+1; begin+=6; break;
		case 83: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta3(begin+4)+1; begin+=7; break;
		case 84: value1+=readDelta3(begin); value2=readDelta1(begin+3); count=readDelta4(begin+4)+1; begin+=8; break;
		case 85: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=1; begin+=5; break;
		case 86: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta1(begin+5)+1; begin+=6; break;
		case 87: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta2(begin+5)+1; begin+=7; break;
		case 88: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta3(begin+5)+1; begin+=8; break;
		case 89: value1+=readDelta3(begin); value2=readDelta2(begin+3); count=readDelta4(begin+5)+1; begin+=9; break;
		case 90: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=1; begin+=6; break;
		case 91: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta1(begin+6)+1; begin+=7; break;
		case 92: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta2(begin+6)+1; begin+=8; break;
		case 93: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta3(begin+6)+1; begin+=9; break;
		case 94: value1+=readDelta3(begin); value2=readDelta3(begin+3); count=readDelta4(begin+6)+1; begin+=10; break;
		case 95: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=1; begin+=7; break;
		case 96: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta1(begin+7)+1; begin+=8; break;
		case 97: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta2(begin+7)+1; begin+=9; break;
		case 98: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta3(begin+7)+1; begin+=10; break;
		case 99: value1+=readDelta3(begin); value2=readDelta4(begin+3); count=readDelta4(begin+7)+1; begin+=11; break;
		case 100: value1+=readDelta4(begin); value2=0; count=1; begin+=4; break;
		case 101: value1+=readDelta4(begin); value2=0; count=readDelta1(begin+4)+1; begin+=5; break;
		case 102: value1+=readDelta4(begin); value2=0; count=readDelta2(begin+4)+1; begin+=6; break;
		case 103: value1+=readDelta4(begin); value2=0; count=readDelta3(begin+4)+1; begin+=7; break;
		case 104: value1+=readDelta4(begin); value2=0; count=readDelta4(begin+4)+1; begin+=8; break;
		case 105: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=1; begin+=5; break;
		case 106: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta1(begin+5)+1; begin+=6; break;
		case 107: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta2(begin+5)+1; begin+=7; break;
		case 108: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta3(begin+5)+1; begin+=8; break;
		case 109: value1+=readDelta4(begin); value2=readDelta1(begin+4); count=readDelta4(begin+5)+1; begin+=9; break;
		case 110: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=1; begin+=6; break;
		case 111: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta1(begin+6)+1; begin+=7; break;
		case 112: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta2(begin+6)+1; begin+=8; break;
		case 113: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta3(begin+6)+1; begin+=9; break;
		case 114: value1+=readDelta4(begin); value2=readDelta2(begin+4); count=readDelta4(begin+6)+1; begin+=10; break;
		case 115: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=1; begin+=7; break;
		case 116: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta1(begin+7)+1; begin+=8; break;
		case 117: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta2(begin+7)+1; begin+=9; break;
		case 118: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta3(begin+7)+1; begin+=10; break;
		case 119: value1+=readDelta4(begin); value2=readDelta3(begin+4); count=readDelta4(begin+7)+1; begin+=11; break;
		case 120: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=1; begin+=8; break;
		case 121: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta1(begin+8)+1; begin+=9; break;
		case 122: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta2(begin+8)+1; begin+=10; break;
		case 123: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta3(begin+8)+1; begin+=11; break;
		case 124: value1+=readDelta4(begin); value2=readDelta4(begin+4); count=readDelta4(begin+8)+1; begin+=12; break;
		}
		(*writer).value1=value1;
		(*writer).value2=value2;
		(*writer).count=count;
//		cout << "value1:" << value1 << "   value2:" << value2 << "   count:" << count<<endl;
		++writer;
	}

	// Update the entries
	pos=triples;
	posLimit=writer;

	return begin;
}

void TwoConstantStatisticsBuffer::flush(){
	buffer->flush();
}
