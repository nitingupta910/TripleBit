#ifndef TIMESTAMP_H_
#define TIMESTAMP_H_

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

#include <sys/time.h>
#include <stdio.h>

class TimeStamp {
	struct timeval start, end;
	double time;
public:
	TimeStamp(){
		time = 0;
	}

	void startTimer() {
		gettimeofday(&start,NULL);
	}

	void endTimer() {
		gettimeofday(&end, NULL);
		time = time + ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000.0;
	}

	void printTime(const char* timername) {
		fprintf(stderr, "%s time used %f ms.\n", timername,time);
	}

	void resetTimer() {
		time = 0;
	}

	virtual ~TimeStamp(){};
};

#endif /* TIMESTAMP_H_ */
