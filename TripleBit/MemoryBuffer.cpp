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

unsigned MemoryBuffer::pagesize = 4096; //4KB

MemoryBuffer::MemoryBuffer()
{

}

MemoryBuffer::MemoryBuffer(unsigned _size) :size(_size) {
	// TODO Auto-generated constructor stub
	buffer = NULL;
	buffer = (char*)malloc(size * sizeof(char));
	if(buffer == NULL) {
		MessageEngine::showMessage("MemoryBuffer::MemoryBuffer, malloc error!", MessageEngine::ERROR);
	}
	::memset(buffer, 0, size);
	currentHead = buffer;
}

MemoryBuffer::~MemoryBuffer() {
	// TODO Auto-generated destructor stub
	//free the buffer
	if(buffer != NULL)
		free(buffer);
	buffer = NULL;
	size = 0;
}

char* MemoryBuffer::resize(unsigned increaseSize)
{
	size_t newsize = size + increaseSize;
	buffer = (char*)realloc(buffer, newsize * sizeof(char));
	if(buffer == NULL) {
		MessageEngine::showMessage("MemoryBuffer::addBuffer, realloc error!", MessageEngine::ERROR);
	}
	currentHead = buffer + size;
	::memset(currentHead, 0, increaseSize);
	size = size + increaseSize;

	return currentHead;
}

Status MemoryBuffer::resize(unsigned increaseSize, bool zero) {
	size_t newsize = increaseSize;
	buffer = (char*)realloc(buffer, newsize * sizeof(char));
	if(buffer == NULL) {
		MessageEngine::showMessage("MemoryBuffer::addBuffer, realloc error!", MessageEngine::ERROR);
		return ERROR;
	}
	currentHead = buffer + size;
	::memset(currentHead, 0, increaseSize - size);
	size = increaseSize;

	return OK;
}

char* MemoryBuffer::getBuffer()
{
	return buffer;
}

void MemoryBuffer::memset(char value)
{
	::memset(buffer, value, size);
}

char* MemoryBuffer::getBuffer(int pos)
{
	return buffer + pos;
}

void MemoryBuffer::save(ofstream& ofile)
{
	ofile<<size<<" ";
	int offset = currentHead - buffer;
	ofile<<offset<<" ";

	unsigned i;
	for(i = 0; i < size; i++) {
		ofile<<buffer[i];
	}
}

void MemoryBuffer::load(ifstream& ifile)
{
	int offset;
	ifile>>size;
	ifile>>offset;

	ifile.get();

	if( buffer != NULL) {
		free(buffer);
		buffer = NULL;
	}

	unsigned remainSize, writeSize, allocSize;
	remainSize = size; writeSize = 0; allocSize = 0;

	if(remainSize >= INIT_PAGE_COUNT * pagesize) {
		buffer = (char*)malloc(INIT_PAGE_COUNT * pagesize);
		writeSize = allocSize = INIT_PAGE_COUNT * pagesize;
	} else {
		buffer = (char*)malloc(remainSize);
		writeSize = allocSize = remainSize;
	}

	if(buffer == NULL) {
		MessageEngine::showMessage("MemoryBuffer::load, malloc error!", MessageEngine::ERROR);
	}

	unsigned i;
	currentHead = buffer;
	while(remainSize > 0) {
		for(i = 0; i < writeSize; i++) {
			//ifile>>currentHead[i];
			currentHead[i] = ifile.get();
		}

		remainSize = remainSize - writeSize;
		if(remainSize >= INIT_PAGE_COUNT * pagesize) {
			buffer = (char*)realloc(buffer, allocSize + INIT_PAGE_COUNT * pagesize);
			writeSize = INIT_PAGE_COUNT * pagesize;
		} else {
			buffer = (char*)realloc(buffer, allocSize + remainSize);
			writeSize = remainSize;
		}

		if(buffer == NULL) {
			MessageEngine::showMessage("MemoryBuffer::load, realloc error!", MessageEngine::ERROR);
		}
		currentHead = buffer + allocSize;
		allocSize = allocSize + writeSize;
	}

	currentHead = buffer + offset;
}

/////////////////////////////////////////////////////////////////////////////////////////
/////// class StatementReificationTable
/////////////////////////////////////////////////////////////////////////////////////////
StatementReificationTable::StatementReificationTable() {
	// TODO Auto-generated constructor stub
	buffer = new MemoryBuffer(REIFICATION_INIT_PAGE_COUNT * MemoryBuffer::pagesize);
	currentBuffer = (ID*)buffer->getBuffer();
	pos = 0;
}

StatementReificationTable::~StatementReificationTable() {
	// TODO Auto-generated destructor stub
	if(buffer != NULL)
		delete buffer;
	buffer = NULL;
}

Status StatementReificationTable::insertStatementReification(ID statement, ID column)
{
	if(REIFICATION_INIT_PAGE_COUNT * getpagesize() / sizeof(ID) <= pos){
		currentBuffer = (ID*)buffer->resize(REIFICATION_INCREMENT_PAGE_COUNT * MemoryBuffer::pagesize);
		pos = 0;
	}

	currentBuffer[pos] = statement;
	pos++;
	currentBuffer[pos] = column;
	pos++;
	return OK;
}

Status StatementReificationTable::getColumn(ID statement, ID& column)
{
	const ID* reader, *limit;
	reader = (ID*)buffer->getBuffer(); limit = (ID*)(buffer->getBuffer() + buffer->getSize());
	while( reader < limit ) {
		if(reader[0] == statement) {
			column = reader[1];
			return OK;
		}
		reader++;
	}

	return REIFICATION_NOT_FOUND;
}


void StatementReificationTable::save(ofstream& ofile)
{
	ofile<<pos<<" ";
	ofile<<(char*)currentBuffer - buffer->getBuffer()<<" ";
	buffer->save(ofile);
}

void StatementReificationTable::load(ifstream& ifile)
{
	unsigned offset;
	ifile>>pos;
	ifile>>offset;
	buffer->load(ifile);

	currentBuffer = (ID*)buffer->getBuffer(offset);
}
