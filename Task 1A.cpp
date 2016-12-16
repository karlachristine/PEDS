#include <string>
#include <iostream>
#include <stdio.h>
#include <stdint.h>
#include <algorithm>

using namespace std;


//

//uint_64 = 9223372036854775807 => 9223372036854775807 9223372036854775807 9223372036854775807\r\n\0 = max. 62 characters
#define BUFFERSIZE 62
#define PACKAGEENTRYS 10000000

struct Request {
	int64_t seq = 0;   // Amount of calls
	int64_t hashid = 0; // Id of request
	int64_t size = 0;   // fiktive size of request
};

// Prototyps
uint64_t inline prozessPackage(FILE *pFile,Request &topRequest, uint64_t &minSize, uint64_t &meanSize, uint64_t &maxSize);
void inline arrayToRequest(const char *pBuffer, Request &request);
void inline quickSortSeq(Request arr[], int64_t left, int64_t right);
void inline quickSortSize(Request arr[], int64_t left, int64_t right);

int main(int64_t argc, char* argv[])
{
	string       traceFileName ;
	FILE        *pFile         = NULL;
	errno_t      err;
	uint64_t     minSize	   = 92233720368547;
	uint64_t     meanSize      = 0;
	uint64_t     maxSize       = 0;
	uint64_t     amount		   = 0;
	Request		 topRequest;

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
	if(err = fopen_s(&pFile, traceFileName.c_str(), "rb") != 0){
		cout << "Datei konnte nicht geoeffnet werden" << endl;
		cout << "Datei: " << traceFileName << endl;
		exit(EXIT_FAILURE);
	}

	cout << "Please stand by calculation will take about 5 min.\n" << endl;


	//Set that only full lines are loaded into the buffer
	setvbuf(pFile, NULL, _IOLBF, BUFFERSIZE);

	// Reading and analysing the file
	uint64_t buffer = 0;
	do 
	{
		buffer = prozessPackage(pFile, topRequest, minSize, meanSize, maxSize);
		amount += buffer;
	}while (buffer == PACKAGEENTRYS);

	fclose(pFile);

	cout << amount << " requests were analysed:" << endl;

	cout << "Smalest object size: " << minSize << " Bytes "<< endl;
	meanSize /= amount;
	cout << "Mean    object size: " << meanSize << " Bytes " << endl;
	cout << "Biggest object size: " << maxSize << " Bytes " << endl;

	cout << "The most popular request is ID: " << topRequest.hashid << endl;
	cout << "#Of calls " << topRequest.seq << endl;
	cout << "Object size of " << topRequest.size << " Bytes" << endl;

	cout << "finish\a" << endl;

	return 0;
	
}

uint64_t inline prozessPackage(FILE *pFile, Request &topRequest, uint64_t &minSize, uint64_t &meanSize, uint64_t &maxSize)
{
	uint64_t     numberOf = 0;
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

	// Calculating most popular Requests
	quickSortSeq(pRequests, 0, numberOf -1);

	if (pRequests[numberOf -1].seq > topRequest.seq) {
		topRequest.seq    = pRequests[numberOf -1].seq;
		topRequest.hashid = pRequests[numberOf-1].hashid;
		topRequest.size   = pRequests[numberOf-1].size;
	}

	// Calculate the fikitv size
	quickSortSize(pRequests, 0, numberOf -1);

	if(pRequests[0].size < minSize) minSize = pRequests[0].size;
	if (pRequests[numberOf-1].size > maxSize) maxSize = pRequests[numberOf-1].size;

	for (uint64_t i = 0; i < numberOf; i++) // Calculate the average size	
		meanSize += pRequests[i].size;

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

// Initialisierung: left = 0; right = array.size
//
void inline quickSortSeq(Request arr[], const int64_t left, const int64_t right) {
	int64_t i     = left;   //all objects smaller than pivot
	int64_t j     = right;  //all objects bigger than pivot
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

// Initialisierung: left = 0; right = array.size
//
void inline quickSortSize(Request arr[], const int64_t left, const int64_t right) {
	int64_t i = left;   //all objects smaller than pivot
	int64_t j = right;  //all objects bigger than pivot
	Request pivot = arr[(left + right) / 2];
	Request tmp;

	/* partition after time */
	while (i <= j) {	// Solange die Linke hälfte Weniger Objekte enthält als die Rechte
		while (arr[i].size < pivot.size) // Alle objekte kleiner gehören in die Linke hälfte
			i++;
		while (arr[j].size > pivot.size)  // Alle objekte größer gehören in die Rechte hälfte
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
		quickSortSize(arr, left, j);
	if (i < right)
		quickSortSize(arr, i, right);
}