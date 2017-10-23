
#include "rm.h"

RelationManager* RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager()
{
	table_id = 1;
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	FileHandle fileHandle;
	DirDescription dirDescription;
	RC rc;
	dirDescription.slotCount = 0;
	dirDescription.freeSpacePointer = 0;
	int pageNum = 1;
	string tableName = "Tables";
	string colName = "Columns";

	// Create table "Tables"
	rbf_manager->createFile(tableName);
	rbf_manager->openFile(tableName, fileHandle);
	char *page=new char[PAGE_SIZE];
	memcpy(page+PAGE_SIZE-sizeof(dirDescription), &dirDescription, sizeof(dirDescription));
	fileHandle.appendPage(page);
	rbf_manager->closeFile(fileHandle);
	delete []page;

	// Create table "Columns"
	rbf_manager->createFile(colName);
	rbf_manager->openFile(colName, fileHandle);
	dirDescription.slotCount = 0;
	dirDescription.freeSpacePointer = 0;
	char *colPage=new char[PAGE_SIZE];
	memcpy(colPage+PAGE_SIZE-sizeof(dirDescription), &dirDescription, sizeof(dirDescription));
	fileHandle.appendPage(colPage);
	rbf_manager->closeFile(fileHandle);
	delete []colPage;

	// Insert the tuple of "Tables" to "Tables"
	rbf_manager->openFile(tableName, fileHandle);
	rc = insertTableTuple(fileHandle, tableName, pageNum);
	rbf_manager->closeFile(fileHandle);

	// Insert attrs of "Tables" to "Columns"
	vector<Attribute> tableAttrs;
	Attribute attr;
	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	tableAttrs.push_back(attr);

	attr.name = "table-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)50;
	tableAttrs.push_back(attr);

	attr.name = "file-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)50;
	tableAttrs.push_back(attr);

	rbf_manager->openFile(colName, fileHandle);
	for(int pos=1; pos<=tableAttrs.size(); pos++){
		rc = insertColTuple(fileHandle, tableAttrs[pos-1], pageNum, pos);
	}
	rbf_manager->closeFile(fileHandle);

	table_id++;

	// Insert the tuple of "Columns" to "Tables"
	rbf_manager->openFile(tableName, fileHandle);
	rc = insertTableTuple(fileHandle, colName, pageNum);
	rbf_manager->closeFile(fileHandle);

	// Insert attr of "Columns" to "Columns"
	vector<Attribute> colAttrs;
	attr.name = "column-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)50;
	colAttrs.push_back(attr);

	attr.name = "column-type";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	colAttrs.push_back(attr);

	attr.name = "column-length";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	colAttrs.push_back(attr);

	attr.name = "column-position";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	colAttrs.push_back(attr);

	rbf_manager->openFile(colName, fileHandle);
	for(int pos=1; pos<=colAttrs.size(); pos++){
		rc = insertColTuple(fileHandle, colAttrs[pos-1], pageNum, pos);
	}
	rbf_manager->closeFile(fileHandle);

	table_id++;

    return 0;
}

RC RelationManager::deleteCatalog()
{
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	rbf_manager->destroyFile("Tables");
	rbf_manager->destroyFile("Columns");
    return 0;
}

bool RelationManager::tableExist(const string &tableName){
	struct stat stFileInfo;

	if(stat(tableName.c_str(), &stFileInfo) == 0) return true;
	else return false;
}

RC RelationManager::insertTableTuple(FileHandle &fileHandle, const string &tableName, int pageNum){
	char* page = new char[PAGE_SIZE];
	//cout << "insertTableTuple getNumberOfPages: " << fileHandle.getNumberOfPages() << endl;
    if(fileHandle.readPage(pageNum,page)!=0){
    		cout << "tableName: " << tableName << endl;
        cout << "readPage from insertTableTuple fail!" << endl;
        return -1;
    }
	DirDescription dirDescription;
	memcpy(&dirDescription,page+PAGE_SIZE-sizeof(DirDescription),sizeof(DirDescription));
	short tableSize = sizeof(int) + sizeof(short) + tableName.length()*2;

	memcpy(page+dirDescription.freeSpacePointer, &table_id, sizeof(int));
	short length = tableName.length();
	memcpy(page+dirDescription.freeSpacePointer+sizeof(int), &length, sizeof(short));
	memcpy(page+dirDescription.freeSpacePointer+sizeof(int)+sizeof(short), &tableName, sizeof(tableName));
	memcpy(page+dirDescription.freeSpacePointer+sizeof(int)+sizeof(short)+sizeof(tableName), &tableName, sizeof(tableName));

	// update dirDescription
	dirDescription.slotCount++;
	dirDescription.freeSpacePointer += tableSize;
	memcpy(page+PAGE_SIZE-sizeof(DirDescription), &dirDescription, sizeof(DirDescription));
	fileHandle.writePage(pageNum,page);

	delete []page;
	return 0;
}

RC RelationManager::insertColTuple(FileHandle &fileHandle, const Attribute &attr, int pageNum, int pos){
	char* page = new char[PAGE_SIZE];
    if(fileHandle.readPage(pageNum,page)!=0){
        cout<<"readPage from insertColTuple fail!"<<endl;
        return -1;
    }
	DirDescription dirDescription;
	memcpy(&dirDescription,page+PAGE_SIZE-sizeof(DirDescription),sizeof(DirDescription));
	short colSize = 0;
	short varcharLength;

	memcpy(page+dirDescription.freeSpacePointer+colSize, &table_id, sizeof(int));
	colSize += sizeof(int);
	varcharLength = attr.name.length();
	memcpy(page+dirDescription.freeSpacePointer+colSize, &varcharLength, sizeof(short));
	colSize += sizeof(short);
	memcpy(page+dirDescription.freeSpacePointer+colSize, &attr.name, varcharLength);
	colSize += varcharLength;
	memcpy(page+dirDescription.freeSpacePointer+colSize, &attr.type, sizeof(int));
	colSize += sizeof(int);
	memcpy(page+dirDescription.freeSpacePointer+colSize, &attr.length, sizeof(int));
	colSize += sizeof(int);
	memcpy(page+dirDescription.freeSpacePointer+colSize, &pos, sizeof(int));
	colSize += sizeof(int);

	// update dirDescription
	dirDescription.slotCount++;
	dirDescription.freeSpacePointer += colSize;
	memcpy(page+PAGE_SIZE-sizeof(DirDescription), &dirDescription, sizeof(DirDescription));
	fileHandle.writePage(pageNum,page);

	delete []page;
	return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	FileHandle fileHandle;
	RC rc;
	rbf_manager->createFile(tableName);
	rbf_manager->openFile(tableName, fileHandle);
	DirDescription dirDescription;
	dirDescription.slotCount = 0;
	dirDescription.freeSpacePointer = 0;
	char *page=new char[PAGE_SIZE];
	memcpy(page+PAGE_SIZE-sizeof(dirDescription), &dirDescription, sizeof(dirDescription));
	fileHandle.appendPage(page);
	rbf_manager->closeFile(fileHandle);
	delete []page;

	int tableSize;
	int colSize=0;
	int pageNum;
	// Insert tuple to table "Tables"
	rbf_manager->openFile("Tables", fileHandle);
	//cout << "getNumberOfPages: " << fileHandle.getNumberOfPages() << endl;
	tableSize = sizeof(int) + sizeof(short) + tableName.length()*2;
	pageNum = rbf_manager->getFreePage(fileHandle, tableSize);
	//cout << "pageNum: " << pageNum << endl;
	insertTableTuple(fileHandle, tableName, pageNum);
	rbf_manager->closeFile(fileHandle);

	// Insert tuple to table "Columns"
	rbf_manager->openFile("Columns", fileHandle);
	for(vector<Attribute>::const_iterator i=attrs.begin(); i!=attrs.end(); i++){
		colSize = colSize + sizeof(int) + sizeof(short) + i->name.length() + sizeof(int)*3;
	}
	pageNum = rbf_manager->getFreePage(fileHandle, colSize);
	for(int pos=1; pos<=attrs.size(); pos++){
		rc = insertColTuple(fileHandle, attrs[pos-1], pageNum, pos);
	}
	rbf_manager->closeFile(fileHandle);
	table_id++;
    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	//RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	FileHandle fileHandle;
    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	FileHandle fileHandle;
	// TODO: how to find which pageNum
	// assume it's page 1 now
	int pageNum=1;
	rbf_manager->openFile("Tables", fileHandle);
	char* page = new char[PAGE_SIZE];
    if(fileHandle.readPage(pageNum,page)!=0){
        cout<<"readPage from getAttributes fail!"<<endl;
        return -1;
    }
    int offset = 0;
    int id;
    short varcharLength;
    string name;
    // Find table id by "Tables"
    while(true){
    		memcpy(&id, page+offset, sizeof(int));
    		memcpy(&varcharLength, page+offset, sizeof(short));
    		offset += sizeof(short);
    		memcpy(&name, page+offset, varcharLength);
    		offset += varcharLength;
    		if(name==tableName)
    			break;
    		offset += varcharLength;
    }
    delete[] page;
    rbf_manager->closeFile(fileHandle);

    // TODO: Find attrs by "Columns"
    rbf_manager->openFile("Columns", fileHandle);
    char* page = new char[PAGE_SIZE];
    if(fileHandle.readPage(pageNum,page)!=0){
        cout<<"readPage from getAttributes fail!"<<endl;
        return -1;
    }
    offset = 0;



    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	FileHandle fileHandle;
	vector<Attribute> recordDescriptor;
	rbf_manager->openFile(tableName, fileHandle);
	getAttributes(tableName, recordDescriptor);
	rbf_manager->insertRecord(fileHandle, recordDescriptor, data, rid);
    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	FileHandle fileHandle;
	vector<Attribute> recordDescriptor;
	rbf_manager->openFile(tableName, fileHandle);
	getAttributes(tableName, recordDescriptor);
	rbf_manager->readRecord(fileHandle, recordDescriptor, rid, data);
    return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	rbf_manager->printRecord(attrs, data);
	return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}


