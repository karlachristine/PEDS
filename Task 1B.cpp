#include <string>
#include <iostream>
#include <stdio.h>
#include <stdint.h>
#include <vector>
#include <algorithm>

using namespace std;


//uint_64 = 9223372036854775807 => 9223372036854775807 9223372036854775807 9223372036854775807\r\n\0 = max. 62 characters
#define BUFFERSIZE    62
#define PACKAGEENTRYS 10000000
#define CACHESIZE     10737418240//1024*1024*1024*10

struct Request {
	uint64_t seq    = 0;   // Amount of calls
	uint64_t hashid = 0;   // Id of request
	uint64_t size   = 0;   // fiktive size of request
};

struct PackageResult {
	uint64_t hitsCache = 0;
	uint64_t sizeCache = 0;
	uint64_t hitsNotCache = 0;
	uint64_t sizeNotCache = 0;
};

// Prototyps
uint64_t inline prozessPackage(FILE *pFile, vector<Request> &cachedRequests, uint64_t &entrys, PackageResult &result);
void inline arrayToRequest(const char *pBuffer, Request &request);
void inline quickSortSeq(Request arr[], int64_t left, int64_t right);
void inline quickSortSeq(vector<Request> &cachedRequests, int64_t left, int64_t right);

int main(int64_t argc, char* argv[])
{
	string			traceFileName = "";
	FILE		   *pFile = NULL;
	errno_t			err;
	uint64_t		entrys  = 0;
	uint64_t		entrysPackage = 0;
	uint64_t		entrysTmp = 0;
	uint64_t		amount = 0;
	vector<Request> cachedRequestsPackage;
	vector<Request> tmpCachedRequests;
	vector<Request> CachedRequests;
	PackageResult	tmpResult;

	// Security check
	if (argc > 0) {
		cout << "Please enter filepath: ";
		getline(cin, traceFileName);// >> traceFileName

		//delete qutoes
		traceFileName.erase(std::remove(traceFileName.begin(), traceFileName.end(), '\"'), traceFileName.end());
	}

	else {
		traceFileName = argv[1]; // Reads filepath as program parameter
	}

	// Opens File
	if (err = fopen_s(&pFile, traceFileName.c_str(), "rb") != 0) {
		cout << "Datei konnte nicht geoeffnet werden" << endl;
		cout << "Datei: " << traceFileName << endl;
		exit(EXIT_FAILURE);
	}

	cout << "Please stand by calculation will take about 5 min.\n" << endl;


	//Set that only full lines are loaded into the buffer
	setvbuf(pFile, NULL, _IOLBF, BUFFERSIZE);

	uint64_t prozessedEntrys = 0;
	// Reading and analysing the file
	do
	{
		prozessedEntrys = prozessPackage(pFile, cachedRequestsPackage, entrysPackage, tmpResult);
		amount += prozessedEntrys;

		// Merge old result with package result Complexity ~ O(n)
		tmpCachedRequests.reserve(entrysPackage + entrys); // preallocate memory 
		tmpCachedRequests.clear();
		tmpCachedRequests.insert(tmpCachedRequests.end(), cachedRequestsPackage.begin(), cachedRequestsPackage.end());
		tmpCachedRequests.insert(tmpCachedRequests.end(), CachedRequests.begin(), CachedRequests.end());
		
		entrysTmp = entrysPackage + entrys;

		// Sort after the fikitv calls
		quickSortSeq(tmpCachedRequests, 0, entrysTmp -1);

		// Sum the size and calls of the chached requests
		uint64_t counter = 0;
		uint64_t sizeCache = 0;
		while (counter < entrysTmp && sizeCache + tmpCachedRequests[entrysTmp - counter- 1].size < CACHESIZE ) {
			sizeCache += tmpCachedRequests[entrysTmp - counter - 1].size;
			counter++;
		}

		// Save new top CACHESIZE requests
		CachedRequests.reserve(counter); // preallocate memory
		CachedRequests.clear();
		CachedRequests.insert(CachedRequests.end(), tmpCachedRequests.end() - counter, tmpCachedRequests.end());
		entrys = counter;

		// Correct result data
		for (uint64_t i = 0; i < entrysTmp -counter;  i++) {
			tmpResult.sizeCache -= tmpCachedRequests[i].size;
			tmpResult.hitsCache -= tmpCachedRequests[i].seq;
			tmpResult.sizeNotCache += tmpCachedRequests[i].size;
			tmpResult.hitsNotCache += tmpCachedRequests[i].seq;
		} 
	} while (prozessedEntrys == PACKAGEENTRYS); // Because if they are less we reached the end of the file

	fclose(pFile);

	cout << amount << "\a requests were analysed" << endl;
	cout << entrys << " requests are cached" << endl;
	cout << "Object hit ratio: "  << tmpResult.hitsCache << " / " << tmpResult.hitsNotCache << " = " << (long float) tmpResult.hitsCache / (long float) tmpResult.hitsNotCache << endl;
	cout << "Object size ratio: " << tmpResult.sizeCache << " / " << tmpResult.sizeNotCache << " = " << (long float) tmpResult.sizeCache / (long float) tmpResult.sizeNotCache << endl;

	return 0;

}

uint64_t inline prozessPackage(FILE *pFile, vector<Request> &cachedRequests, uint64_t &entrys, PackageResult &result)
{
	uint64_t      numberOf = 0;
	char         buffer[BUFFERSIZE] = "";
	Request     *pRequests = new Request[PACKAGEENTRYS];

	// Security check
	if (pFile == nullptr) {
		cout << "Error with File occured" << endl;
		exit(EXIT_FAILURE);
	}

	// Convert char array in numbers
	for (uint64_t i = 0; i < PACKAGEENTRYS && fgets(buffer, BUFFERSIZE, pFile)/*!feof(pFile)*/; i++, numberOf++)
	{
		arrayToRequest(buffer, pRequests[i]);
	}


	// Sort after calls
	quickSortSeq(pRequests, 0, numberOf-1);

	// Sum the size and calls of the chached requests // Sum 10GB from top
	uint64_t counter = 0;
	uint64_t sizeCache = 0;
	while (sizeCache + pRequests[numberOf - counter-1].size < CACHESIZE && counter < numberOf) {
		sizeCache += pRequests[numberOf - counter-1].size;
		result.sizeCache += pRequests[numberOf - counter-1].size;
		result.hitsCache += pRequests[numberOf - counter-1].seq;
		counter++;
	}
	
	entrys = counter;
	cachedRequests.resize(entrys);
	
	// Collect the cached requests
	for (uint64_t j = 0; j < entrys; j++) {
		cachedRequests[j].seq = pRequests[numberOf - j-1].seq;
		cachedRequests[j].size = pRequests[numberOf - j-1].size;
		cachedRequests[j].hashid = pRequests[numberOf - j-1].hashid;
	}

	// Sum the size and calls of the NOT chached requests // Sum the rest from the Bottom
	for (uint64_t j = 0; j < numberOf-entrys; j++) {
		result.sizeNotCache += pRequests[j].size;
		result.hitsNotCache += pRequests[j].seq;
	}

	delete pRequests;
	return numberOf;
}

void inline arrayToRequest(const char *pBuffer, Request &request) {
	char seqNumberBuffer[20] = "";
	char hashidNumberBuffer[20] = "";
	char sizeNumberBuffer[20] = "";
	int  offset = 0;

	// Get number of request calls
	while (offset < BUFFERSIZE && pBuffer[offset] < '0' || pBuffer[offset] >'9') offset++; // Removing whitespace

	for (int j = offset; j < BUFFERSIZE && j < 20; j++) {   // Select number characters

		if (pBuffer[j + offset] >= '0' && pBuffer[j + offset] <= '9') {
			seqNumberBuffer[j] = pBuffer[j + offset];
		}
		else {
			offset += j;
			break;
		}
	}

	request.seq = _atoi64(seqNumberBuffer);

	// Get number of hashid
	while (offset < BUFFERSIZE && pBuffer[offset] < '0' || pBuffer[offset] >'9') offset++; // Removing whitespace

	for (int j = 0; j < BUFFERSIZE && j < 20; j++) {   // Select number characters

		if (pBuffer[j + offset] >= '0' && pBuffer[j + offset] <= '9') {
			hashidNumberBuffer[j] = pBuffer[j + offset];
		}
		else {
			offset += j;
			break;
		}
	}

	request.hashid = _atoi64(hashidNumberBuffer);

	// Get number of size
	while (offset < BUFFERSIZE && pBuffer[offset] < '0' || pBuffer[offset] >'9') offset++; // Removing whitespace

	for (int j = 0; j < BUFFERSIZE && j < 20; j++) {   // Select number characters

		if (pBuffer[j + offset] >= '0' && pBuffer[j + offset] <= '9') {
			sizeNumberBuffer[j] = pBuffer[j + offset];
		}
		else {
			offset += j;
			break;
		}
	}

	request.size = _atoi64(sizeNumberBuffer);
}


// Initialisierung: left = 0; right = array.size-1
//
void inline quickSortSeq(Request arr[], const int64_t left, const int64_t right) {
	int64_t i = left;   //all objects smaller than pivot
	int64_t j = right;  //all objects bigger than pivot
	Request pivot = arr[(left + right) / 2];
	Request tmp;

	/* partition after time */
	while (i <= j) {	// Solange die Linke hälfte Weniger Objekte enthält als die Rechte
		while (arr[i].seq < pivot.seq) // Alle objekte kleiner gehören in die Linke hälfte
			i++;
		while (arr[j].seq > pivot.seq)  // Alle objekte größer gehören in die Rechte hälfte
			j--;
		if (i <= j) {					// Wenn die linke hälfte immernoch kleiner Left & Right object austauschen
			tmp = arr[i];
			arr[i] = arr[j];
			arr[j] = tmp;
			i++;
			j--;
		}
	};

	/* recursion */
	if (left < j)
		quickSortSeq(arr, left, j);
	if (i < right)
		quickSortSeq(arr, i, right);
}

// Initialisierung: left = 0; right = array.size-1
//
void inline quickSortSeq(vector<Request> &arr, const int64_t left, const int64_t right) {
	int64_t i = left;   //all objects smaller than pivot
	int64_t j = right;  //all objects bigger than pivot
	Request pivot = arr[(left + right) / 2];
	Request tmp;

	/* partition after time */
	while (i <= j) {	// Solange die Linke hälfte Weniger Objekte enthält als die Rechte
		while (arr[i].seq < pivot.seq) // Alle objekte kleiner gehören in die Linke hälfte
			i++;
		while (arr[j].seq > pivot.seq)  // Alle objekte größer gehören in die Rechte hälfte
			j--;
		if (i <= j) {					// Wenn die linke hälfte immernoch kleiner Left & Right object austauschen
			tmp = arr[i];
			arr[i] = arr[j];
			arr[j] = tmp;
			i++;
			j--;
		}
	};

	/* recursion */
	if (left < j)
		quickSortSeq(arr, left, j);
	if (i < right)
		quickSortSeq(arr, i, right);
}