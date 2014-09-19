#ifndef _TEMPFILE_H_
#define _TEMPFILE_H_

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
#include <fstream>
#include <string>
//---------------------------------------------------------------------------
#if defined(_MSC_VER)
typedef unsigned __int64 uint64_t;
#else
#include <stdint.h>
#endif
//---------------------------------------------------------------------------
/// A temporary file
class TempFile {
private:
	/// The next id
	static unsigned id;

	/// The base file name
	std::string baseName;
	/// The file name
	std::string fileName;
	/// The output
	std::ofstream out;

	/// The buffer size
	static const unsigned bufferSize = 16384;
	/// The write buffer
	char writeBuffer[bufferSize];
	/// The write pointer
	unsigned writePointer;

	/// Construct a new suffix
	static std::string newSuffix();

public:
	/// Constructor
	TempFile(const std::string& baseName);
	/// Destructor
	~TempFile();

	/// Get the base file name
	const std::string& getBaseFile() const {
		return baseName;
	}
	/// Get the file name
	const std::string& getFile() const {
		return fileName;
	}

	/// Flush the file
	void flush();
	/// Close the file
	void close();
	/// Discard the file
	void discard();

	/// Write a string
	void writeString(unsigned len, const char* str);
	/// Write a id
	/// flag==0 subject
	/// flag==1 object
	/// flag==2 predicate
	void writeId(ID id, unsigned char flag);
	void writeId(ID id);
	/// Raw write
	void write(unsigned len, const char* data);

	/// Skip a predicate
	static const char* skipId(const char* reader);
	/// Skip a string
	static const char* skipString(const char* reader);
	/// Read an id
	static const char* readId(const char* reader, ID& id);
	/// Read a string
	static const char* readString(const char* reader, unsigned& len, const char*& str);
};

//----------------------------------------------------------------------------
/// Maps a file read-only into memory
class MemoryMappedFile
{
   private:
   /// os dependent data
   struct Data;

   /// os dependen tdata
   Data* data;
   /// Begin of the file
   const char* begin;
   /// End of the file
   const char* end;

   public:
   /// Constructor
   MemoryMappedFile();
   /// Destructor
   ~MemoryMappedFile();

   /// Open
   bool open(const char* name);
   /// Close
   void close();

   /// Get the begin
   const char* getBegin() const { return begin; }
   /// Get the end
   const char* getEnd() const { return end; }

   /// Ask the operating system to prefetch a part of the file
   void prefetch(const char* start,const char* end);
};
//---------------------------------------------------------------------------
#endif
