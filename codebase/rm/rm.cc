
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
	FileHandle tableFileHandle, colFileHandle;
	DirDescription dirDescription;
	RC rc;
	dirDescription.slotCount = 0;
	dirDescription.freeSpacePointer = 0;
	int pageNum = 1;
	string tableName = "Tables";
	string colName = "Columns";

	// Create table "Tables"
	rbf_manager->createFile(tableName);
	rbf_manager->openFile(tableName, tableFileHandle);
	//cout << "old getNumberOfPages: " << tableFileHandle.getNumberOfPages() << endl;
	char *page=new char[PAGE_SIZE];
	memcpy(page+PAGE_SIZE-sizeof(dirDescription), &dirDescription, sizeof(dirDescription));
	tableFileHandle.appendPage(page);
	//cout << "new getNumberOfPages: " << tableFileHandle.getNumberOfPages() << endl;
	rbf_manager->closeFile(tableFileHandle);
	//page = NULL;
	delete []page;

	// Insert the tuple of "Tables" and "Columns" to "Tables"
	rbf_manager->openFile(tableName, tableFileHandle);
	rc = insertTableTuple(tableFileHandle, tableName, pageNum);
	table_id = 2;
	rc = insertTableTuple(tableFileHandle, colName, pageNum);
	rbf_manager->closeFile(tableFileHandle);

	// Create table "Columns"
	rbf_manager->createFile(colName);
	rbf_manager->openFile(colName, colFileHandle);
	dirDescription.slotCount = 0;
	dirDescription.freeSpacePointer = 0;
	char *colPage=new char[PAGE_SIZE];
	memcpy(colPage+PAGE_SIZE-sizeof(dirDescription), &dirDescription, sizeof(dirDescription));
	colFileHandle.appendPage(colPage);
	colFileHandle.getNumberOfPages();
	rbf_manager->closeFile(colFileHandle);
	//colPage = NULL;
	delete []colPage;

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

	rbf_manager->openFile(colName, colFileHandle);
	table_id = 1;
	for(int pos=1; pos<=tableAttrs.size(); pos++){
		rc = insertColTuple(colFileHandle, tableAttrs[pos-1], pageNum, pos);
	}
	rbf_manager->closeFile(colFileHandle);

	// Insert attr of "Columns" to "Columns"
	vector<Attribute> colAttrs;
	attr.name = "table-id";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)50;
	colAttrs.push_back(attr);

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

	rbf_manager->openFile(colName, colFileHandle);
	table_id = 2;
	for(int pos=1; pos<=colAttrs.size(); pos++){
		rc = insertColTuple(colFileHandle, colAttrs[pos-1], pageNum, pos);
	}
	rbf_manager->closeFile(colFileHandle);

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
	//cout << "tableName: " << tableName << endl;
	//cout << "insertTableTuple getNumberOfPages: " << fileHandle.getNumberOfPages() << endl;
	//cout << "pageNum: " << pageNum << endl;
    if(fileHandle.readPage(pageNum,page)!=0){
    		cout << "tableName: " << tableName << endl;
        cout << "readPage from insertTableTuple fail!" << endl << endl;
        return -1;
    }
	DirDescription dirDescription;
	memcpy(&dirDescription,page+PAGE_SIZE-sizeof(DirDescription),sizeof(DirDescription));
	short tableSize = sizeof(int) + sizeof(short) + tableName.length()*2;
	const char* name = tableName.c_str();

	//cout << "table_id: " << table_id << endl;
	memcpy(page+dirDescription.freeSpacePointer, &table_id, sizeof(int));
	short length = tableName.size();
	//cout << "length: " << length << endl;
	memcpy(page+dirDescription.freeSpacePointer+sizeof(int), &length, sizeof(short));
	memcpy(page+dirDescription.freeSpacePointer+sizeof(int)+sizeof(short), name, length);
	memcpy(page+dirDescription.freeSpacePointer+sizeof(int)+sizeof(short)+length, name, length);

	// update dirDescription
	dirDescription.slotCount++;
	dirDescription.freeSpacePointer += tableSize;
	memcpy(page+PAGE_SIZE-sizeof(DirDescription), &dirDescription, sizeof(DirDescription));
	fileHandle.writePage(pageNum,page);

	//page = NULL;
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
	const char* attrName = attr.name.c_str();
	//cout << "attrName: " << attrName << endl;

	memcpy(page+dirDescription.freeSpacePointer+colSize, &table_id, sizeof(int));
	colSize += sizeof(int);
	varcharLength = attr.name.length();
	//cout<<"varcharLength: "<<varcharLength<<endl;
	memcpy(page+dirDescription.freeSpacePointer+colSize, &varcharLength, sizeof(short));
	colSize += sizeof(short);
	memcpy(page+dirDescription.freeSpacePointer+colSize, attrName, varcharLength);
	//cout<<"varcharLength: "<<varcharLength<<endl;
	//cout << "attr.name: " << attr.name<< endl;
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

	//page = NULL;
	delete []page;
	return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	FileHandle fileHandle, tableFileHandle, colFileHandle;
	RC rc;
	rbf_manager->createFile(tableName);
	rbf_manager->openFile(tableName, fileHandle);
	//fileHandle.getNumberOfPages();
	DirDescription dirDescription;
	dirDescription.slotCount = 0;
	dirDescription.freeSpacePointer = 0;
	char *page=new char[PAGE_SIZE];
	memcpy(page+PAGE_SIZE-sizeof(dirDescription), &dirDescription, sizeof(dirDescription));
	fileHandle.appendPage(page);
	//cout << "createTable fileHandle.getNumberOfPages: " << fileHandle.getNumberOfPages() << endl;
	rbf_manager->closeFile(fileHandle);
	//page = NULL;
	delete []page;

	int tableSize;
	int colSize=0;
	int pageNum;
	// Insert tuple to table "Tables"
	rbf_manager->openFile("Tables", tableFileHandle);
	//cout << "getNumberOfPages: " << tableFileHandle.getNumberOfPages() << endl;
	tableSize = sizeof(int) + sizeof(short) + tableName.length()*2;
	pageNum = rbf_manager->getFreePage(tableFileHandle, tableSize);
	//cout << "pageNum: " << pageNum << endl;
	insertTableTuple(tableFileHandle, tableName, pageNum);
	rbf_manager->closeFile(tableFileHandle);

	// Insert tuple to table "Columns"
	rbf_manager->openFile("Columns", colFileHandle);
	for(vector<Attribute>::const_iterator i=attrs.begin(); i!=attrs.end(); i++){
		colSize = colSize + sizeof(int) + sizeof(short) + i->name.length() + sizeof(int)*3;
	}
	pageNum = rbf_manager->getFreePage(colFileHandle, colSize);
	//cout << "pageNum: " << pageNum << endl;
	for(int pos=1; pos<=attrs.size(); pos++){
		rc = insertColTuple(colFileHandle, attrs[pos-1], pageNum, pos);
	}
	rbf_manager->closeFile(colFileHandle);
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
	FileHandle tableFileHandle, colFileHandle;
	Attribute attr;
	int offset, id, findID;
	short varcharLength;
	char* name = NULL;
	const char* TableName = tableName.c_str();
	rbf_manager->openFile("Tables", tableFileHandle);

	char* page = new char[PAGE_SIZE];
	// find tableName is in which page of "Tables" and its table_id
	//cout << "tableFileHandle.getNumberOfPages: " << tableFileHandle.getNumberOfPages() << endl;
	//cout << "tableName: " << tableName << endl;
	for(int pageNum=1; pageNum<=tableFileHandle.getNumberOfPages(); pageNum++){
		//cout << "pageNum: " << pageNum << endl;
		if(tableFileHandle.readPage(pageNum,page)!=0){
			cout<<"readPage from getAttributes fail!"<<endl;
			return -1;
		}
		offset = 0;
		// Find table id by "Tables"
		while(true){
			memcpy(&id, page+offset, sizeof(int));
			//cout << "id: " << id << endl;
			offset += sizeof(int);
			memcpy(&varcharLength, page+offset, sizeof(short));
			//cout << "varcharLength: " << varcharLength << endl;
			offset += sizeof(short);
			name = new char[varcharLength];
			memcpy(name, page+offset, varcharLength);
			//cout << "tableName in Tables: " << name << endl;
			offset += varcharLength;
			if(strcmp(name, TableName)==0)
				break;
			offset += varcharLength;
			name = NULL;
			delete []name;
			// Not in this page
			if(offset > PAGE_SIZE-sizeof(DirDescription))
				break;
		}
		if(strcmp(name, TableName)==0){
			name = NULL;
			delete []name;
			break;
		}
	}
	page = NULL;
	delete[] page;
	rbf_manager->closeFile(tableFileHandle);


    // Find attrs by "Columns"
	//id=3;
    rbf_manager->openFile("Columns", colFileHandle);
    char* colPage = new char[PAGE_SIZE];
    for(int pageNum=1; pageNum<=colFileHandle.getNumberOfPages(); pageNum++){
		if(colFileHandle.readPage(pageNum,colPage)!=0){
			cout<<"readPage from getAttributes fail!"<<endl;
			return -1;
		}
		// find position(offset) of table in "Columns" by table_id
		offset = 0;
		while(true){
			memcpy(&findID, colPage+offset, sizeof(int));
			//cout << "findID: " << findID << endl;
			offset += sizeof(int);
			if(findID==id)
				break;
			memcpy(&varcharLength, colPage+offset, sizeof(short));
			//cout << "varcharLength: " << varcharLength << endl;
			offset += sizeof(short) + varcharLength + sizeof(int)*3;
			// not in this page
			if(offset > PAGE_SIZE-sizeof(DirDescription))
				break;
		}
		if(findID==id)
			break;
    }
	// find attrs
    char* attrName = NULL;
    int pos;
	while(findID==id){
		memcpy(&varcharLength, colPage+offset, sizeof(short));
		//cout << "varcharLength: " << varcharLength << endl;
		offset += sizeof(short);
		attrName = new char[varcharLength];
		memcpy(attrName, colPage+offset, varcharLength);
		//cout << "attrName: " << attrName << endl;
		attr.name = attrName;
		attrName = NULL;
		delete []attrName;
		//cout << "name: " << attr.name << endl;
		offset += varcharLength;
		memcpy(&attr.type, colPage+offset, sizeof(int));
		offset += sizeof(int);
		memcpy(&attr.length, colPage+offset, sizeof(int));
		offset += sizeof(int);
		memcpy(&pos, colPage+offset, sizeof(int));
		//cout << "pos: " << pos << endl;
		offset += sizeof(int);
		attrs.push_back(attr);
		memcpy(&findID, colPage+offset, sizeof(int));
		offset += sizeof(int);
		//cout << "findID: " << findID << endl;
	}

    rbf_manager->closeFile(colFileHandle);
    colPage = NULL;
    delete []colPage;
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


