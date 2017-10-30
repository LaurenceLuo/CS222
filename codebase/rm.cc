
#include "rm.h"

RelationManager* RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager()
{

}

RelationManager::~RelationManager()
{
}

void prepareTableRecord(const int table_id, const int nameLength, const string &table_name, const string &file_name, void *buffer)
{
    int offset = 0;

    int nullFieldsIndicator = 0;
    // Null-indicator for the fields
    memcpy((char *)buffer + offset, &nullFieldsIndicator, sizeof(int));
    offset += 1;

    // table_id
    memcpy((char *)buffer + offset, &table_id, sizeof(int));
    offset += sizeof(int);

    // table_name
    memcpy((char *)buffer + offset, &nameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, &table_name, nameLength);
    offset += nameLength;

    // file_name
    memcpy((char *)buffer + offset, &nameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, &file_name, nameLength);
    offset += nameLength;
}

void prepareColRecord(const int table_id, const int nameLength, const string &col_name, const int col_type, const int col_length, const int col_pos, void *buffer)
{
    int offset = 0;

    // Null-indicator for the fields
    memset(buffer, 0, 8);
    offset += 2;

    // table_id
    memcpy((char *)buffer + offset, &table_id, sizeof(int));
    offset += sizeof(int);

    // col_name
    memcpy((char *)buffer + offset, &nameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, &col_name, nameLength);
    offset += nameLength;

    // col_type
    memcpy((char *)buffer + offset, &col_type, sizeof(int));
    offset += sizeof(int);

    // col_length
    memcpy((char *)buffer + offset, &col_length, sizeof(int));
    offset += sizeof(int);

    // col_pos
    memcpy((char *)buffer + offset, &col_pos, sizeof(int));
    offset += sizeof(int);
}

RC RelationManager::createCatalog()
{
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	FileHandle tableFileHandle, colFileHandle;
	DirDescription dirDescription;
	RID rid;
	dirDescription.slotCount = 1;
	dirDescription.freeSpacePointer = 0;
	string tableName = "Tables";
	string colName = "Columns";

	// Create table "Tables"
	rbf_manager->createFile(tableName);
	rbf_manager->openFile(tableName, tableFileHandle);

	vector<Attribute> recordDescriptor;
    Attribute attr;
    attr.name = "table_id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "table_name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    recordDescriptor.push_back(attr);

    attr.name = "file_name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    recordDescriptor.push_back(attr);

    void *record = malloc(100);
    prepareTableRecord(1, 6, tableName, tableName, record);
    rbf_manager->insertRecord(tableFileHandle, recordDescriptor, record, rid);
    free(record);

    record = malloc(100);
    prepareTableRecord(2, 7, colName, colName, record);
    rbf_manager->insertRecord(tableFileHandle, recordDescriptor, record, rid);
    free(record);
	rbf_manager->closeFile(tableFileHandle);

	// Create table "Columns"
	rbf_manager->createFile(colName);
	rbf_manager->openFile(colName, colFileHandle);

	// table attr
	vector<Attribute> tableAttrs;
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

	for(int pos=1; pos<=3; pos++){
		record = malloc(100);
		prepareColRecord(1, tableAttrs[pos-1].name.length(), tableAttrs[pos-1].name, tableAttrs[pos-1].type, tableAttrs[pos-1].length, pos, record);
		rbf_manager->insertRecord(colFileHandle, colAttrs, record, rid);
		free(record);
	}

	for(int pos=1; pos<=3; pos++){
		record = malloc(100);
		prepareColRecord(2, colAttrs[pos-1].name.length(), colAttrs[pos-1].name, colAttrs[pos-1].type, colAttrs[pos-1].length, pos, record);
		rbf_manager->insertRecord(colFileHandle, colAttrs, record, rid);
		free(record);
	}

	rbf_manager->closeFile(colFileHandle);

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
	char* page = NULL;
	page = new char[PAGE_SIZE];
	memset(page, 0, PAGE_SIZE);
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

	//cout << "table_id: " << dirDescription.slotCount << endl;
	int table_id = (int)dirDescription.slotCount;
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

	// update direction page
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	int freeSpace = PAGE_SIZE - sizeof(DirDescription) - dirDescription.freeSpacePointer;
	RID rid;
	rid.pageNum = pageNum;
	rid.slotNum = 0;
	rbf_manager->updateDirectoryPage(fileHandle, rid, freeSpace);

	page = NULL;
	delete []page;
	return 0;
}

RC RelationManager::insertColTuple(FileHandle &fileHandle, const Attribute &attr, int pageNum, int pos){
	char* page = NULL;
	page = new char[PAGE_SIZE];
	memset(page, 0, PAGE_SIZE);
	//cout << "pageNum: " << pageNum << endl;
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

	//cout << "table_id: " << dirDescription.slotCount << endl;
	//cout << "freeSpacePointer: " << dirDescription.freeSpacePointer << endl;
	int table_id = (int)dirDescription.slotCount;
	memcpy(page+dirDescription.freeSpacePointer+colSize, &table_id, sizeof(int));
	colSize += sizeof(int);
	varcharLength = attr.name.length();
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
	dirDescription.freeSpacePointer += colSize;
	memcpy(page+PAGE_SIZE-sizeof(DirDescription), &dirDescription, sizeof(DirDescription));
	fileHandle.writePage(pageNum,page);

	// update direction page
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	int freeSpace = PAGE_SIZE - sizeof(DirDescription) - dirDescription.freeSpacePointer;
	RID rid;
	rid.pageNum = pageNum;
	rid.slotNum = 0;
	rbf_manager->updateDirectoryPage(fileHandle, rid, freeSpace);

	page = NULL;
	delete []page;
	return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	FileHandle fileHandle, tableFileHandle, colFileHandle;
	RID rid;
	int table_id;
	DirDescription dirDescription;
	rbf_manager->createFile(tableName);

	// Insert tuple to table "Tables"
	vector<Attribute> tableAttrs;
    Attribute attr;
    attr.name = "table_id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    tableAttrs.push_back(attr);

    attr.name = "table_name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tableAttrs.push_back(attr);

    attr.name = "file_name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tableAttrs.push_back(attr);

	rbf_manager->openFile("Tables", tableFileHandle);
    void *record = malloc(100);
    // TODO: table_id
    char *page = NULL;
    page = new char[PAGE_SIZE];
    memset(page, 0, PAGE_SIZE);
    tableFileHandle.readPage(1, page);
    memcpy(&dirDescription, page+PAGE_SIZE-sizeof(dirDescription), sizeof(dirDescription));
    table_id = (int)dirDescription.slotCount;

    prepareTableRecord(table_id, tableName.length(), tableName, tableName, record);
    rbf_manager->insertRecord(tableFileHandle, tableAttrs, record, rid);
    free(record);

	rbf_manager->closeFile(tableFileHandle);

	// Insert tuple to table "Columns"
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

	rbf_manager->openFile("Columns", colFileHandle);
	for(int pos=1; pos<=attrs.size(); pos++){
		record = malloc(100);
		prepareColRecord(table_id, attrs[pos-1].name.length(), attrs[pos-1].name, attrs[pos-1].type, attrs[pos-1].length, pos, record);
		rbf_manager->insertRecord(colFileHandle, colAttrs, record, rid);
		free(record);
	}

	rbf_manager->closeFile(colFileHandle);
    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	if(tableName.compare("Tables")==0 or tableName.compare("Columns")==0){
		cout << "System catalog cannot be deleted!" << endl;
		return -1;
	}
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	FileHandle tableFileHandle, colFileHandle;
	DirDescription dirDescription;
	RID rid;
	int offset, beginOffset, shiftLength, recordSize;
	int findID, id;
	int stop=0;
	short varcharLength;
	char* name = NULL;
	const char* TableName = tableName.c_str();
	rbf_manager->destroyFile(tableName);

	rbf_manager->openFile("Tables", tableFileHandle);
	char* tablePage = NULL;
	for(int pageNum=1; pageNum<=tableFileHandle.getNumberOfPages(); pageNum++){
		tablePage = new char[PAGE_SIZE];
		memset(tablePage, 0, PAGE_SIZE);
		if(tableFileHandle.readPage(pageNum,tablePage)!=0){
			cout<<"readPage from getAttributes fail!"<<endl;
			return -1;
		}
		offset = 0;
		// find table offset
		while(true){
			memcpy(&id, tablePage+offset, sizeof(int));
			//cout << "id: " << id << endl;
			offset += sizeof(int);
			memcpy(&varcharLength, tablePage+offset, sizeof(short));
			//cout << "varcharLength: " << varcharLength << endl;
			offset += sizeof(short);
			name = new char[varcharLength];
			memset(name, 0, varcharLength+1);
			memcpy(name, tablePage+offset, varcharLength);
			cout << "tableName in Tables: " << name << endl;
			offset += varcharLength;
			if(memcmp(name, TableName, tableName.length())==0)
				break;
			offset += varcharLength;
			name = NULL;
			delete []name;
			// Not in this page
			if(offset > PAGE_SIZE-sizeof(DirDescription))
				break;
		}
		// delete record in "Tables" and shift
		if(memcmp(name, TableName, tableName.length())==0){
			memcpy(&dirDescription, tablePage+PAGE_SIZE-sizeof(DirDescription), sizeof(DirDescription));
			beginOffset = offset - (varcharLength+sizeof(short)+sizeof(int));
			offset += varcharLength;
			recordSize = offset - beginOffset;
			shiftLength = dirDescription.freeSpacePointer - offset;
			char* shiftContent = new char[shiftLength];
			memset(shiftContent, 0, shiftLength);
			memcpy(shiftContent, tablePage+offset, shiftLength);
			memcpy(tablePage+beginOffset, shiftContent, shiftLength);
			dirDescription.freeSpacePointer -= recordSize;
			memcpy(tablePage+dirDescription.freeSpacePointer, &stop, sizeof(int));
			memcpy(tablePage+PAGE_SIZE-sizeof(DirDescription), &dirDescription, sizeof(DirDescription));
			tableFileHandle.writePage(pageNum, tablePage);
			rid.pageNum = pageNum;
			rid.slotNum = 0;
			shiftContent = NULL;
			delete []shiftContent;
			tablePage = NULL;
			delete []tablePage;
			name = NULL;
			delete []name;
			break;
		}
		tablePage = NULL;
		delete []tablePage;
	}
	// update direction page
	int freeSpace = PAGE_SIZE - sizeof(DirDescription) - dirDescription.freeSpacePointer;
	rbf_manager->updateDirectoryPage(tableFileHandle, rid, freeSpace);

	rbf_manager->closeFile(tableFileHandle);
	//cout << "delete record " << tableName << " in Tables success" << endl;

    rbf_manager->openFile("Columns", colFileHandle);
    int colPageNum;
    char* colPage = NULL;
    char* shiftContent = NULL;
    for(int pageNum=1; pageNum<=colFileHandle.getNumberOfPages(); pageNum++){
    		colPage = new char[PAGE_SIZE];
    		memset(colPage, 0, PAGE_SIZE);
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
			if(findID==id){
				colPageNum = pageNum;
				break;
			}
			memcpy(&varcharLength, colPage+offset, sizeof(short));
			//cout << "varcharLength: " << varcharLength << endl;
			offset += sizeof(short) + varcharLength + sizeof(int)*3;
			// not in this page
			if(offset > PAGE_SIZE-sizeof(DirDescription))
				break;
		}
		if(findID==id)
			break;
		colPage = NULL;
		delete []colPage;
    }

	// delete record in "Columns" and shift
	while(findID==id){
		//cout << "findID: " << findID << endl;
		memcpy(&dirDescription, colPage+PAGE_SIZE-sizeof(DirDescription), sizeof(DirDescription));
		beginOffset = offset - sizeof(int);
		//cout << "beginOffset: " << beginOffset << endl;
		memcpy(&varcharLength, colPage+offset, sizeof(short));
		//cout << "varcharLength: " << varcharLength << endl;
		offset += sizeof(short) + varcharLength + sizeof(int)*3;
		//cout << "offset: " << offset << endl;
		recordSize = offset - beginOffset;
		//cout << "recordSize: " << recordSize << endl;
		shiftLength = dirDescription.freeSpacePointer - offset;
		//cout << "shiftLength: " << shiftLength << endl;
		shiftContent = new char[shiftLength];
		memset(shiftContent, 0, shiftLength);
		memcpy(shiftContent, colPage+offset, shiftLength);
		memcpy(colPage+beginOffset, shiftContent, shiftLength);
		dirDescription.freeSpacePointer -= recordSize;
		//cout << "freeSpacePointer: " << dirDescription.freeSpacePointer << endl;
		memcpy(colPage+dirDescription.freeSpacePointer, &stop, sizeof(int));
		memcpy(colPage+PAGE_SIZE-sizeof(DirDescription), &dirDescription, sizeof(DirDescription));
		colFileHandle.writePage(colPageNum, colPage);
		rid.pageNum = colPageNum;
		rid.slotNum = 0;
		shiftContent = NULL;
		delete []shiftContent;
		memcpy(&findID, colPage+beginOffset, sizeof(int));
		//cout << "findID: " << findID << endl;
		offset = beginOffset + sizeof(int);
	}

	// update direction page
	freeSpace = PAGE_SIZE - sizeof(DirDescription) - dirDescription.freeSpacePointer;
	rbf_manager->updateDirectoryPage(colFileHandle, rid, freeSpace);

    rbf_manager->closeFile(colFileHandle);
    //cout << "delete record " << tableName << " in Columns success" << endl;
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

	// find tableName is in which page of "Tables" and its table_id
	//cout << "tableFileHandle.getNumberOfPages: " << tableFileHandle.getNumberOfPages() << endl;
	//cout << "tableName: " << tableName << endl;
	for(int pageNum=1; pageNum<=tableFileHandle.getNumberOfPages(); pageNum++){
		char* tablePage = NULL;
		tablePage = new char[PAGE_SIZE];
		memset(tablePage, 0, PAGE_SIZE);
		if(tableFileHandle.readPage(pageNum,tablePage)!=0){
			cout << "pageNum: " << pageNum << endl;
			cout<<"readPage from getAttributes fail!"<<endl;
			return -1;
		}
		offset = 0;
		// Find table id by "Tables"
		while(true){
			memcpy(&id, tablePage+offset, sizeof(int));
			//cout << "id: " << id << endl;
			offset += sizeof(int);
			memcpy(&varcharLength, tablePage+offset, sizeof(short));
			//cout << "varcharLength: " << varcharLength << endl;
			offset += sizeof(short);
			name = NULL;
			name = new char[varcharLength];
			memset(name, 0, varcharLength+1);
			memcpy(name, tablePage+offset, varcharLength);
			string str_name = name;
			//cout << "strName: " << str_name << endl;
			//cout << "tableName in Tables: " << name << "," << TableName << endl;
			//cout << name << endl;
			offset += varcharLength;
			//cout << "cmp: " << memcmp(name, TableName, tableName.length()) << endl;
			if(memcmp(name, TableName, tableName.length())==0){
				break;
			}
			offset += varcharLength;
			name = NULL;
			delete []name;
			// Not in this page
			if(offset > PAGE_SIZE-sizeof(DirDescription))
				break;
		}
		if(memcmp(name, TableName, tableName.length())==0){
			tablePage = NULL;
			delete []tablePage;
			name = NULL;
			delete []name;
			break;
		}
		tablePage = NULL;
		delete []tablePage;
	}
	rbf_manager->closeFile(tableFileHandle);

    // Find attrs by "Columns"
    rbf_manager->openFile("Columns", colFileHandle);
    char* colPage = NULL;
    for(int pageNum=1; pageNum<=colFileHandle.getNumberOfPages(); pageNum++){
    		colPage = new char[PAGE_SIZE];
    		memset(colPage, 0, PAGE_SIZE);
		if(colFileHandle.readPage(pageNum,colPage)!=0){
			cout << "pageNum: " << pageNum << endl;
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
		colPage = NULL;
		delete []colPage;
    }
	// find attrs

    int pos;
	while(findID==id){
		char* attrName = NULL;
		attrName = new char[varcharLength];
		memcpy(&varcharLength, colPage+offset, sizeof(short));
		//cout << "varcharLength: " << varcharLength << endl;
		offset += sizeof(short);
		memset(attrName, 0, varcharLength+1);
		memcpy(attrName, colPage+offset, varcharLength);
		//cout << "attrName: " << attrName << endl;
		attr.name = attrName;
		attrName=NULL;
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
	colPage = NULL;
	delete []colPage;
    rbf_manager->closeFile(colFileHandle);
    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	FileHandle fileHandle;
	RC rc;
	vector<Attribute> recordDescriptor;
	rc = rbf_manager->openFile(tableName, fileHandle);
	if(rc){
		cout << "openFile in insertTuple in Table: " << tableName << " failed!" << endl;
		return -1;
	}
	getAttributes(tableName, recordDescriptor);
	// test recordDescriptor
	/*
    for(unsigned i = 0; i < recordDescriptor.size(); i++){
        cout << (i+1) << ". Attr Name: " << recordDescriptor[i].name << " Type: " << (AttrType) recordDescriptor[i].type << " Len: " << recordDescriptor[i].length << endl;
    }
    */
	rc = rbf_manager->insertRecord(fileHandle, recordDescriptor, data, rid);
	if(rc){
		cout << "insertRecord in insertTuple in Table: " << tableName << " failed!" << endl;
		return -1;
	}
	rc = rbf_manager->closeFile(fileHandle);
	if(rc){
		cout << "closeFile in insertTuple in Table: " << tableName << " failed!" << endl;
		return -1;
	}
    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	FileHandle fileHandle;
	RC rc;
	vector<Attribute> recordDescriptor;
	rc = rbf_manager->openFile(tableName, fileHandle);
	if(rc){
		cout << "openFile in deleteTuple in Table: " << tableName << " failed!" << endl;
		return -1;
	}
	//cout << "openFile suc in deleteTuple!" << endl;
	getAttributes(tableName, recordDescriptor);
	//cout << "getAttr suc in deleteTuple!" << endl;
	rc = rbf_manager->deleteRecord(fileHandle, recordDescriptor, rid);
	//cout << "delete suc!" << endl;
	if(rc){
		cout << "deleteRecord in deleteTuple in Table: " << tableName << " failed!" << endl;
		return -1;
	}
	rc = rbf_manager->closeFile(fileHandle);
	if(rc){
		cout << "closeFile in deleteTuple in Table: " << tableName << " failed!" << endl;
		return -1;
	}
    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	FileHandle fileHandle;
	RC rc;
	vector<Attribute> recordDescriptor;
	rc = rbf_manager->openFile(tableName, fileHandle);
	if(rc){
		cout << "openFile in updateTuple in Table: " << tableName << " failed!" << endl;
		return -1;
	}
	getAttributes(tableName, recordDescriptor);
	rc = rbf_manager->updateRecord(fileHandle, recordDescriptor, data, rid);
	//cout << "updateRecord suc!" << endl;
	if(rc){
		cout << "updateRecord in updateTuple in Table: " << tableName << " failed!" << endl;
		return -1;
	}
	rc = rbf_manager->closeFile(fileHandle);
	if(rc){
		cout << "closeFile in updateTuple in Table: " << tableName << " failed!" << endl;
		return -1;
	}
    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	FileHandle fileHandle;
	RC rc;
	vector<Attribute> recordDescriptor;
	rc = rbf_manager->openFile(tableName, fileHandle);
	//cout << "open RM success" << endl;
	if(rc){
		cout << "openFile in readTuple in Table: " << tableName << " failed!" << endl;
		return -1;
	}
	//cout << tableName << endl;
	getAttributes(tableName, recordDescriptor);
	//cout << "getAttributes success" << endl;
	/*
    for(unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        cout << (i+1) << ". Attr Name: " << recordDescriptor[i].name << " Type: " << (AttrType) recordDescriptor[i].type << " Len: " << recordDescriptor[i].length << endl;
    }
    */

	rc = rbf_manager->readRecord(fileHandle, recordDescriptor, rid, data);
	if(rc){
		cout << "readRecord in readTuple in Table: " << tableName << " failed!" << endl;
		return -1;
	}
	//cout << "read RM success" << endl;
	rc = rbf_manager->closeFile(fileHandle);
	if(rc){
		cout << "closeFile in readTuple in Table: " << tableName << " failed!" << endl;
		return -1;
	}
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
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	FileHandle fileHandle;
	RC rc;
	vector<Attribute> recordDescriptor;
	rc = rbf_manager->openFile(tableName, fileHandle);
	if(rc){
		cout << "openFile in readAttribute in Table: " << tableName << " failed!" << endl;
		return -1;
	}
	getAttributes(tableName, recordDescriptor);
	rc = rbf_manager->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
	if(rc){
		cout << "readAttribute in readAttribute in Table: " << tableName << " failed!" << endl;
		return -1;
	}
	rc = rbf_manager->closeFile(fileHandle);
	if(rc){
		cout << "closeFile in readAttribute in Table: " << tableName << " failed!" << endl;
		return -1;
	}
    return 0;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
	FileHandle fileHandle;
	RC rc;
	vector<Attribute> recordDescriptor;
	rc = rbf_manager->openFile(tableName, fileHandle);
	if(rc){
		cout << "openFile in scan in Table: " << tableName << " failed!" << endl;
		return -1;
	}
	getAttributes(tableName, recordDescriptor);

	rc = rbf_manager->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_iterator);
	if(rc){
		cout << "scan in Table: " << tableName << " failed!" << endl;
		return -1;
	}
    return 0;
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


