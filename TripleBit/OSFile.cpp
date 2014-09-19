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

#include "OSFile.h"

#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/uio.h>

OSFile::OSFile() {
	// TODO Auto-generated constructor stub

}

OSFile::~OSFile() {
	// TODO Auto-generated destructor stub
}


bool OSFile::fileExists(const string filename)
{
	struct stat sbuff;
	if( stat(filename.c_str(),&sbuff) == 0 ){
		if( S_ISREG(sbuff.st_mode) )
			return true;
	}
	return false;
}


bool OSFile::directoryExists(const string path)
{
	struct stat sbuff;
	if( stat(path.c_str(),&sbuff) == 0 ){
		cout<<"asdfasf"<<endl;
		if( S_ISDIR(sbuff.st_mode) )
			return true;
	}
	return false;
}

bool OSFile::mkdir(const string path)
{
	return (::mkdir( path.c_str(), S_IRWXU|S_IRWXG|S_IRWXO ) == 0);
}

size_t OSFile::fileSize(std::string filename)
{
	struct stat buf;
	stat(filename.c_str(), &buf);
	return buf.st_size;
}
