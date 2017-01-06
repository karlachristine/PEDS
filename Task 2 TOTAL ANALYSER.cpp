// This is an extansion of TASK I B
// This code uses multithreading as descripted in:
// https://solarianprogrammer.com/2011/12/16/cpp-11-thread-tutorial/
// 
// This code useses also a modified form of quicksort to fit the struct Request

#include <string>
#include <iostream>
#include <stdio.h>
#include <stdint.h>
#include <vector>
#include <algorithm>
#include <thread>
#include <unordered_map>
#include <map>
#include <chrono>
#include <ctime>
#include <fstream>

#define _CRT_SECURE_NO_WARNINGS

using namespace std;


//uint_64 = 9223372036854775807 => 9223372036854775807 9223372036854775807 9223372036854775807\r\n\0 = max. 62 characters ~ 62 to be sure
#define BUFFERSIZE    62 
#define PACKAGEENTRYS 10000000
#define CACHESIZE     107374182400 // 1024*1024*1024*100
#define NUMOFTHREADS  2
#define NUMOFFILES    7
#define MAXPOPULAR	  10000 // 1mio

struct Request {
	int64_t seq = 0;   // Amount of calls
	int64_t hashid = 0;   // Id of request
	int64_t size = 0;   // fiktive size of request
};

class PopularData {
public:
	PopularData::PopularData() {}
	PopularData::~PopularData() {}

	void	PopularData::insertData(uint64_t key, uint64_t value) {
		auto it = m_DataByKey.find(key);
		if (it != m_DataByKey.end())
			it->second += value;
		else {
			m_DataByKey.insert({ key, value });
			it = m_DataByKey.find(key);
			// m_DataSortedSeq.insert(it->second), it->first); Verursacht fehler
			std::pair<uint64_t, const uint64_t*> buffer;
			buffer.first = (it->second);
			buffer.second = &(it->first);
			m_DataSortedSeq.insert(buffer);
		}
	}

	void	PopularData::deleteSmalestData() {
		auto it = m_DataSortedSeq.begin();
		uint64_t key = *it->second;
		m_DataSortedSeq.erase(it);
		m_DataByKey.erase(key);
	}

	unordered_map<uint64_t, uint64_t> m_DataByKey;        // Key = hashid , Value = seq
	multimap<uint64_t, const uint64_t*>    m_DataSortedSeq;  // Key = seq, Value = id
};

// Prototypes
void prozessFile(string traceFileName, PopularData &popular, PopularData &popular100, uint64_t &amount, unordered_map<uint64_t, uint64_t> &histogram);
uint64_t inline prozessPackage(FILE *pFile, PopularData &popular, PopularData &popular100, unordered_map<uint64_t, uint64_t> &histogram);
void inline arrayToRequest(const char *pBuffer, Request &request);
void inline quickSortSeq(Request arr[], int64_t left, int64_t right);
void inline quickSortSeq(vector<Request> &cachedRequests, int64_t left, int64_t right);

int main()
{
	string					               traceFileName = "";
	string					               traceFileDirectory = "";
	uint64_t				               amount = 0;
	unordered_map<uint64_t, uint64_t>      histogram;
	PopularData							   mostPopular();
	uint64_t				               tmpAmount[NUMOFFILES];
	unordered_map<uint64_t, uint64_t>      tmpHistogram[NUMOFFILES];
	vector<PopularData>					   tmpMostPopular100(NUMOFFILES);
    vector<PopularData>					   tmpMostPopular(NUMOFFILES);
	vector<vector<uint64_t>>			   resultComparisson;

	// Enter files
	cout << "Please enter filepath without file: ";
	getline(cin, traceFileDirectory);
	cout << "Please enter filename: ";
	getline(cin, traceFileName);

	// Delete qutoes
	traceFileDirectory.erase(std::remove(traceFileDirectory.begin(), traceFileDirectory.end(), '\"'), traceFileDirectory.end());
	traceFileName.erase(std::remove(traceFileName.begin(), traceFileName.end(), '\"'), traceFileName.end());
	
	// Get & Show Date, Time
	std::chrono::time_point<std::chrono::system_clock> start, end;
	start = std::chrono::system_clock::now();
	std::time_t start_time = std::chrono::system_clock::to_time_t(start);
	cout << std::ctime(&start_time) << endl;

	// Create threads
	std::thread thread[NUMOFFILES]; 

	// Launch thread
	for (int i = 0; i < NUMOFFILES; i += NUMOFTHREADS)
	{
		for (int j = 0; j < NUMOFTHREADS && j+i < NUMOFFILES; ++j) {
			cout << "Thread " << i+j << " launched" << endl;
			traceFileName[10] = '1' + i+j;
			thread[j] = std::thread(prozessFile, traceFileDirectory + traceFileName,std::ref(tmpMostPopular[i+j]), std::ref(tmpMostPopular100[i + j]), std::ref(tmpAmount[i + j]), std::ref(tmpHistogram[i + j]));
		}

		// Join the threads with the main thread
		for (int j = 0; j < NUMOFTHREADS && j + i < NUMOFFILES; ++j) {
			thread[j].join();
			cout << "Thread " << i+j << " finished" << endl;
		}
	}

	// Prozess the Results of each thread
	for (int i = 0; i < NUMOFFILES; i++) {
		
		amount += tmpAmount[i];

		// Merge histogram data
		for (auto it = tmpHistogram[i].begin(); it != tmpHistogram[i].end(); ++it) {
			if (histogram[it->first])
				histogram[it->first] += it->second;
			else
				histogram[it->first] = it->second;
		}
	}

	// Compare the change from day to day
	resultComparisson.resize(NUMOFFILES - 1);

	for (int i = 0; i < NUMOFFILES -1; i++) {
		
		resultComparisson[i].resize(NUMOFFILES - i - 1);
		
			for (int j = i + 1; j < NUMOFFILES; j++) {
			
				resultComparisson[i][j - i -1] = 0;
				
				//compare TmpMostPop[i] with uTmpMostPop[j]
				for (auto it = tmpMostPopular100[i].m_DataByKey.begin(); it != tmpMostPopular100[i].m_DataByKey.end(); ++it){
					if ((tmpMostPopular100[j].m_DataByKey.find(it->first)) != tmpMostPopular100[j].m_DataByKey.end())
						resultComparisson[i][j - i - 1]++;
				}
		}
	}

	// Get & Show Date, Time
	end = std::chrono::system_clock::now();
	std::time_t end_time = std::chrono::system_clock::to_time_t(end);
	cout << std::ctime(&end_time) << endl;
	cout << amount << "\a requests were analysed\n" << endl;

	cout << "Comparisson of the daily 100 most Popular requests" << endl;
	cout << "-----------------------------------------------" << endl;
	cout << "Moday too Tuesday : " << resultComparisson[0][0] << endl;
	cout << "Moday too Wendsday: " << resultComparisson[0][1] << endl;
	cout << "Moday too Thursday: " << resultComparisson[0][2] << endl;
	cout << "Moday too Friday  : " << resultComparisson[0][3] << endl;
	cout << "Moday too Saturday: " << resultComparisson[0][4] << endl;
	cout << "Moday too Sunday  : " << resultComparisson[0][5] << endl;
	cout << "-----------------------------------------------" << endl;
	cout << "Tuesday too Wendsday: " << resultComparisson[1][0] << endl;
	cout << "Tuesday too Thursday: " << resultComparisson[1][1] << endl;
	cout << "Tuesday too Friday  : " << resultComparisson[1][2] << endl;
	cout << "Tuesday too Saturday: " << resultComparisson[1][3] << endl;
	cout << "Tuesday too Sunday  : " << resultComparisson[1][4] << endl;
	cout << "-----------------------------------------------" << endl;
	cout << "Wendsday too Thursday: " << resultComparisson[2][0] << endl;
	cout << "Wendsday too Friday  : " << resultComparisson[2][1] << endl;
	cout << "Wendsday too Saturday: " << resultComparisson[2][2] << endl;
	cout << "Wendsday too Sunday  : " << resultComparisson[2][3] << endl;
	cout << "-----------------------------------------------" << endl;
	cout << "Thursday too Friday  : " << resultComparisson[3][0] << endl;
	cout << "Thursday too Saturday: " << resultComparisson[3][1] << endl;
	cout << "Thursday too Sunday  : " << resultComparisson[3][2] << endl;
	cout << "-----------------------------------------------" << endl;
	cout << "Friday too Saturday: " << resultComparisson[4][0] << endl;
	cout << "Friday too Sunday  : " << resultComparisson[4][1] << endl;
	cout << "-----------------------------------------------" << endl;
	cout << "Saturday too Sunday  : " << resultComparisson[5][0] << endl;

	
	
	std::map<uint64_t, uint64_t> orderedHistogram(histogram.begin(), histogram.end());

	cout << "\nHistogram Data" << endl;
	cout << "Number of different request sizes: " << histogram.size() << endl;

	ofstream onfile;
	string   fileName = "histogram-daten.txt";
	onfile.open(traceFileDirectory+fileName);

	onfile << "size" << "," << "count" << "\n";

	for (auto it = orderedHistogram.begin(); it != orderedHistogram.end(); ++it)
		onfile << it->first << ","<< it->second<< "\n";
	onfile.close();

	
	for (int i = 0; i < NUMOFFILES; i++) {

		ofstream onfile;
		string   fileName = "TopMostPopular.07.01.txt";
		fileName[19] = '1' + i;
		onfile.open(traceFileDirectory + fileName);

		onfile << "seq" << "," << "N";

		int j = 1;
		for (auto it = tmpMostPopular[i].m_DataSortedSeq.rbegin(); it != tmpMostPopular[i].m_DataSortedSeq.rend(); ++it, j++)
			onfile << it->first << "," << j <<"\n";
		onfile.close();
	}

	return 0;

}

void prozessFile(string traceFileName , PopularData &popular, PopularData &popular100, uint64_t &amount, unordered_map<uint64_t, uint64_t> &histogram)
{
	FILE		   *pFile = nullptr;
	errno_t			err;

	amount = 0;

	// Opens File
	if (err = fopen_s(&pFile, traceFileName.c_str(), "rb") != 0) {
		cout << "File couldn't be open" << endl;
		cout << "File: " << traceFileName << endl;
		exit(EXIT_FAILURE);
	}

	//Set that only full lines are loaded into the buffer
	setvbuf(pFile, NULL, _IOLBF, BUFFERSIZE);

	uint64_t prozessedEntrys = 0;
	// Reading and analysing the file
	do
	{
		prozessedEntrys = prozessPackage(pFile, popular, popular100, histogram);
		amount += prozessedEntrys;

		// Resize popular Data 
		while (popular100.m_DataSortedSeq.size() > 100)
			popular100.deleteSmalestData();

		while (popular.m_DataSortedSeq.size() > MAXPOPULAR)
			popular.deleteSmalestData();


	}while (prozessedEntrys == PACKAGEENTRYS);

	fclose(pFile);
}

uint64_t inline prozessPackage(FILE *pFile, PopularData &popular, PopularData &popular100, unordered_map<uint64_t, uint64_t> &histogram)
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
	
	// Sort after the fikitv calls
	quickSortSeq(pRequests, 0, numberOf - 1);

	unordered_map<uint64_t, uint64_t>::iterator it;
	// Collect Histogram Data
	for (int i = 0; i < numberOf; i++) {
		it = histogram.find(pRequests[i].size);
		if (it != histogram.end())
			it->second++;
		else
			histogram.insert({ pRequests[i].size, 1 });
	}

	// Collect the 10^6-most popular
	for (int i = 0; i < 100 && i < numberOf; i++)
	{
		popular100.insertData(pRequests[i].hashid, pRequests[i].size);
	}

	// Collect the 10^6-most popular
	for (int i = 0; i < MAXPOPULAR && i < numberOf; i++)
	{
		popular.insertData(pRequests[i].hashid, pRequests[i].size);
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