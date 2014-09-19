#ifndef SORTER_H
#define SORTER_H

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

class TempFile;

/// Sort a temporary file
class Sorter {
   public:
   /// Sort a file
   static void sort(TempFile& in,TempFile& out,const char* (*skip)(const char*),int (*compare)(const char*,const char*),bool eliminateDuplicates=false);
};
#endif /*SOTER_H*/
