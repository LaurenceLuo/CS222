#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h> 
#include <string.h>
#include <stdexcept>
#include <stdio.h> 

#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

using namespace std;

int RBFTest_8g(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Multiple Records
    // 4. Scan
    // 5. Float Compare
    // 6. Print Records
    // 7. Close Record-Based File
    // 8. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 8 *****" << endl;
   
    RC rc;
    string fileName = "test8g";

    // Create a file named "test8"
    rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file should not fail.");

    // Open the file "test8"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");
      
    RID rid; 
    int recordSize = 0;
    void *record = malloc(100);
    //void *returnedData = malloc(100);

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);
    
    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert a record into a file and print the record
    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", 25, 172.8, 6200, record, &recordSize);
    cout << "Inserting Record:" << endl;
    rbfm->printRecord(recordDescriptor, record);
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
    
    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", 25, 173.8, 6200, record, &recordSize);
    cout << "Inserting Record:" << endl;
    rbfm->printRecord(recordDescriptor, record);
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
    
    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", 25, 0, 6200, record, &recordSize);
    cout << "Inserting Record:" << endl;
    rbfm->printRecord(recordDescriptor, record);
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
    
    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", 25, 177.8, 6200, record, &recordSize);
    cout << "Inserting Record:" << endl;
    rbfm->printRecord(recordDescriptor, record);
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
    
    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", 25, 175.8, 6200, record, &recordSize);
    cout << "Inserting Record:" << endl;
    rbfm->printRecord(recordDescriptor, record);
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == success && "Inserting a record should not fail.");
    
    RBFM_ScanIterator rmsi;
    vector<string> attrs;
    attrs.push_back("EmpName");
    attrs.push_back("Age");
    attrs.push_back("Height");
    attrs.push_back("Salary");
    
    float value=175.8;
    rc = rbfm->scan(fileHandle, recordDescriptor, "", NO_OP, NULL, attrs, rmsi);
    assert(rc == success && "RecordBasedFileManager::scan() should not fail.");
    RID _rid;
    void *returnedData = malloc(4000);
    while(rmsi.getNextRecord(_rid, returnedData) != RBFM_EOF){
        cout << endl << "NO_OP Data:" << endl;
        rbfm->printRecord(recordDescriptor, returnedData);
    }

    rc = rbfm->scan(fileHandle, recordDescriptor, "Height", LT_OP, &value, attrs, rmsi);
    assert(rc == success && "RecordBasedFileManager::scan() should not fail.");
    while(rmsi.getNextRecord(_rid, returnedData) != RBFM_EOF){
        cout << endl << "LT_OP 175.8 Data:" << endl;
        rbfm->printRecord(recordDescriptor, returnedData);
    }
    
    rc = rbfm->scan(fileHandle, recordDescriptor, "Height", LE_OP, &value, attrs, rmsi);
    assert(rc == success && "RecordBasedFileManager::scan() should not fail.");
    while(rmsi.getNextRecord(_rid, returnedData) != RBFM_EOF){
        cout << endl << "LE_OP 175.8 Data:" << endl;
        rbfm->printRecord(recordDescriptor, returnedData);
    }
    
    rc = rbfm->scan(fileHandle, recordDescriptor, "Height", GT_OP, &value, attrs, rmsi);
    assert(rc == success && "RecordBasedFileManager::scan() should not fail.");
    while(rmsi.getNextRecord(_rid, returnedData) != RBFM_EOF){
        cout << endl << "GT_OP 175.8 Data:" << endl;
        rbfm->printRecord(recordDescriptor, returnedData);
    }
    
    rc = rbfm->scan(fileHandle, recordDescriptor, "Height", GE_OP, &value, attrs, rmsi);
    assert(rc == success && "RecordBasedFileManager::scan() should not fail.");
    while(rmsi.getNextRecord(_rid, returnedData) != RBFM_EOF){
        cout << endl << "GE_OP 175.8 Data:" << endl;
        rbfm->printRecord(recordDescriptor, returnedData);
    }
    
    rc = rbfm->scan(fileHandle, recordDescriptor, "Height", NE_OP, &value, attrs, rmsi);
    assert(rc == success && "RecordBasedFileManager::scan() should not fail.");
    while(rmsi.getNextRecord(_rid, returnedData) != RBFM_EOF){
        cout << endl << "NE_OP 175.8 Data:" << endl;
        rbfm->printRecord(recordDescriptor, returnedData);
    }

    rc = rbfm->scan(fileHandle, recordDescriptor, "Height", EQ_OP, &value, attrs, rmsi);
    assert(rc == success && "RecordBasedFileManager::scan() should not fail.");
    while(rmsi.getNextRecord(_rid, returnedData) != RBFM_EOF){
        cout << endl << "EQ_OP 175.8 Data:" << endl;
        rbfm->printRecord(recordDescriptor, returnedData);
    }

    // Given the rid, read the record from file
    //rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
    //assert(rc == success && "Reading a record should not fail.");

    //cout << endl << "Returned Data:" << endl;
    //rbfm->printRecord(recordDescriptor, returnedData);

    // Compare whether the two memory blocks are the same
    if(memcmp(record, returnedData, recordSize) != 0)
    {
        cout << "[FAIL] Test Case 8g Failed!" << endl << endl;
        free(record);
        free(returnedData);
        return -1;
    }
    
    cout << endl;

    // Close the file "test8g"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    // Destroy the file
    rc = rbfm->destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success  && "Destroying the file should not fail.");
    
    free(record);
    free(returnedData);

    cout << "RBF Test Case 8g Finished! The result will be examined." << endl << endl;
    
    return 0;
}

int main()
{
    // To test the functionality of the record-based file manager 
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); 
     
    remove("test8g");
       
    RC rcmain = RBFTest_8g(rbfm);
    return rcmain;
}
