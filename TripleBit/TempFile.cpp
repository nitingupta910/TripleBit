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

#include "TempFile.h"
#include <sstream>
#include <cassert>
#include <cstring>

using namespace std;
//---------------------------------------------------------------------------
/// The next id
unsigned TempFile::id = 0;
//---------------------------------------------------------------------------
string TempFile::newSuffix()
// Construct a new suffix
{
	stringstream buffer;
	buffer << '.' << (id++);
	return buffer.str();
}
//---------------------------------------------------------------------------
TempFile::TempFile(const string& baseName) :
	baseName(baseName), fileName(baseName + newSuffix()), out(fileName.c_str(), ios::out | ios::binary | ios::trunc), writePointer(0)
// Constructor
{
}
//---------------------------------------------------------------------------
TempFile::~TempFile()
// Destructor
{
//	discard();
}
//---------------------------------------------------------------------------
void TempFile::flush()
// Flush the file
{
	if (writePointer) {
		out.write(writeBuffer, writePointer);
		writePointer = 0;
	}
	out.flush();
}
//---------------------------------------------------------------------------
void TempFile::close()
// Close the file
{
	flush();
	out.close();
}
//---------------------------------------------------------------------------
void TempFile::discard()
// Discard the file
{
	close();
	remove(fileName.c_str());
}
//---------------------------------------------------------------------------
void TempFile::writeString(unsigned len, const char* str)
// Write a string
{
	writeId(len);
	write(len, str);
}
//---------------------------------------------------------------------------
void TempFile::writeId(ID id, unsigned char flag)
// Write a id
{

	while (id >= 128) {
		unsigned char c = static_cast<unsigned char> (id | (flag<<8));
		if (writePointer == bufferSize) {
			out.write(writeBuffer, writePointer);
			writePointer = 0;
		}
		writeBuffer[writePointer++] = c;
		id >>= 7;
	}
	if (writePointer == bufferSize) {
		out.write(writeBuffer, writePointer);
		writePointer = 0;
	}
	writeBuffer[writePointer++] = static_cast<unsigned char> (id | (flag<<8));

}
//---------------------------------------------------------------------------
void TempFile::writeId(ID id)
// Write a id
{
	for(int i = 0 ; i < 4; i++) {
		unsigned char c = static_cast<unsigned char> (id | 0);
		if (writePointer == bufferSize) {
			out.write(writeBuffer, writePointer);
			writePointer = 0;
		}
		writeBuffer[writePointer++] = c;
		id >>= 8;
	}

}
//---------------------------------------------------------------------------
void TempFile::write(unsigned len, const char* data)
// Raw write
{
	// Fill the buffer
	if (writePointer + len > bufferSize) {
		unsigned remaining = bufferSize - writePointer;
		memcpy(writeBuffer + writePointer, data, remaining);
		out.write(writeBuffer, bufferSize);
		writePointer = 0;
		len -= remaining;
		data += remaining;
	}
	// Write big chunks if any
	if (writePointer + len > bufferSize) {
		assert(writePointer==0);
		unsigned chunks = len / bufferSize;
		out.write(data, chunks * bufferSize);
		len -= chunks * bufferSize;
		data += chunks * bufferSize;
	}
	// And fill the rest
	memcpy(writeBuffer + writePointer, data, len);
	writePointer += len;
}
//---------------------------------------------------------------------------
const char* TempFile::skipId(const char* reader)
// Skip an id
{
	return reader + 4;
}

//---------------------------------------------------------------------------
const char* TempFile::readId(const char* reader, ID& id)
// Read an id
{
	id = 0;
	unsigned shift = 0;
	for(int i = 0; i < 4; i++) {
		unsigned char c = *reinterpret_cast<const unsigned char*> (reader++);
		id |= static_cast<ID> (c) << shift;
		shift += 8;
	}
	return reader;
}

////////////////////////////////////////////////////////////////////////////////////////

#if defined(WIN32)||defined(__WIN32__)||defined(_WIN32)
#define CONFIG_WINDOWS
#endif
#ifdef CONFIG_WINDOWS
#include <windows.h>
#else
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#endif

#include <stdio.h>
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//----------------------------------------------------------------------------
// OS dependent data
struct MemoryMappedFile::Data
{
#ifdef CONFIG_WINDOWS
   /// The file
   HANDLE file;
   /// The mapping
   HANDLE mapping;
#else
   /// The file
   int file;
   /// The mapping
   void* mapping;
#endif
};
//----------------------------------------------------------------------------
MemoryMappedFile::MemoryMappedFile()
   : data(0),begin(0),end(0)
   // Constructor
{
}
//----------------------------------------------------------------------------
MemoryMappedFile::~MemoryMappedFile()
   // Destructor
{
   close();
}
//----------------------------------------------------------------------------
bool MemoryMappedFile::open(const char* name)
   // Open
{
   if (!name) return false;
   close();

   #ifdef CONFIG_WINDOWS
      HANDLE file=CreateFile(name,GENERIC_READ,FILE_SHARE_READ,0,OPEN_EXISTING,0,0);
      if (file==INVALID_HANDLE_VALUE) return false;
      DWORD size=GetFileSize(file,0);
      HANDLE mapping=CreateFileMapping(file,0,PAGE_READONLY,0,size,0);
      if (mapping==INVALID_HANDLE_VALUE) { CloseHandle(file); return false; }
      begin=static_cast<char*>(MapViewOfFile(mapping,FILE_MAP_READ,0,0,size));
      if (!begin) { CloseHandle(mapping); CloseHandle(file); return false; }
      end=begin+size;
   #else
      int file=::open(name,O_RDONLY);
      if (file<0) return false;
      size_t size=lseek(file,0,SEEK_END);
      if (!(~size)) { ::close(file); return false; }
      void* mapping=mmap(0,size,PROT_READ,MAP_PRIVATE,file,0);
      if (mapping == MAP_FAILED) {
		::close(file);
		return false;
      }
      begin=static_cast<char*>(mapping);
      end=begin+size;
   #endif
   data=new Data();
   data->file=file;
   data->mapping=mapping;

   return true;
}
//----------------------------------------------------------------------------
void MemoryMappedFile::close()
   // Close
{
   if (data) {
#ifdef CONFIG_WINDOWS
      UnmapViewOfFile(const_cast<char*>(begin));
      CloseHandle(data->mapping);
      CloseHandle(data->file);
#else
      munmap(data->mapping,end-begin);
      ::close(data->file);
#endif
      delete data;
      data=0;
      begin=end=0;
   }
}
unsigned sumOfItAll;
//----------------------------------------------------------------------------
void MemoryMappedFile::prefetch(const char* start,const char* end)
   // Ask the operating system to prefetch a part of the file
{
   if ((end<start)||(!data))
      return;

#ifdef CONFIG_WINDOWS
   // XXX todo
#else
   posix_fadvise(data->file,start-begin,end-start+1,POSIX_FADV_WILLNEED);
#endif
}
//----------------------------------------------------------------------------
