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

int RBFTest_8(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record
    // 4. Read Attribute
    // 5. Close Record-Based File
    // 6. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 8 *****" << endl;
   
    RC rc;
    string fileName = "test8e";

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
    void *returnedData = malloc(100);

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);
    
    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert a record into a file and print the record
    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", 25, 177.8, 6200, record, &recordSize);
    cout << endl << "Inserting Data:" << endl;
    rbfm->printRecord(recordDescriptor, record);
    
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == success && "Inserting a record should not fail.");
    
    // Given the rid and attribute name, read the record attribute from file
    //cout<<"rid.pageNum: "<<rid.pageNum<<" rid.slotNum: "<<rid.slotNum<<endl;
    char* name=new char[8];
    rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, "EmpName", name);
    assert(rc == success && "ReadAttribute a record should not fail.");

    cout << endl << "Returned Data:" << endl;
    cout<<"EmpName: ";
    char* _data=name;
    for(int i=0;i<8;i++){
        cout<<*_data;
        _data++;
    }
    cout<<endl;
    
    int age;
    rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, "Age", &age);
    assert(rc == success && "Read a attribute should not fail.");
    cout<<"Age: "<<age<<endl;
    
    float height;
    rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, "Height", &height);
    assert(rc == success && "Read a attribute should not fail.");
    cout<<"Height: "<<height<<endl;
    
    int salary;
    rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, "Salary", &salary);
    assert(rc == success && "Read a attribute should not fail.");
    cout<<"Salary: "<<salary<<endl;
    
    // Compare whether the two memory blocks are the same
    if(memcmp((char*)record+nullFieldsIndicatorActualSize+sizeof(int), name, 8) != 0)
    {
        cout << "[FAIL] Test Case 8e Failed!" << endl << endl;
        free(record);
        free(returnedData);
        return -1;
    }
    
    cout << endl;
    
    int notExisting;
    rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, "NotExisting", &notExisting);
    assert(rc != success && "ReadAttribute a non-existing attribute should fail.");

    // Close the file "test8"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    // Destroy the file
    rc = rbfm->destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success  && "Destroying the file should not fail.");
    
    free(record);
    free(returnedData);

    cout << "RBF Test Case 8e Finished! The result will be examined." << endl << endl;
    
    return 0;
}

int main()
{
    // To test the functionality of the record-based file manager 
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); 
     
    remove("test8e");
       
    RC rcmain = RBFTest_8(rbfm);
    return rcmain;
}
