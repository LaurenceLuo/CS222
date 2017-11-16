
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	PagedFileManager *pfm_manager=PagedFileManager::instance();
	return pfm_manager->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName)
{
	PagedFileManager *pfm_manager=PagedFileManager::instance();
	return pfm_manager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	PagedFileManager *pfm_manager=PagedFileManager::instance();
    if(ixfileHandle.fileHandle._isOpen())
        return -1;
    if(pfm_manager->openFile(fileName,ixfileHandle.fileHandle)!=0){
        cout<< "File " << fileName << " open fail!" << endl << endl;
        return -1;
    }
    ixfileHandle.fileHandle.getNumberOfPages();
    return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	PagedFileManager *pfm_manager=PagedFileManager::instance();
	return pfm_manager->closeFile(ixfileHandle.fileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = ixReadPageCounter;
	writePageCount = ixWritePageCounter;
	appendPageCount = ixAppendPageCounter;
    return 0;
}

RC BtreeNode::initData(void *data){
    //nodeID=0;
    memcpy(nodePage,(char*)data,PAGE_SIZE);
    
    memcpy(&nodeID, nodePage, sizeof(int));
    
    //nodeType=Index;
    memcpy(&nodeType, nodePage + sizeof(int), sizeof(int));
    
    //attrType=TypeInt;
    memcpy(&attrType, nodePage + 2 * sizeof(int), sizeof(int));
    
    //int attrLen = 0;
    memcpy(&attrLen, nodePage + 3 * sizeof(int), sizeof(int));
    
    //int deleteMark = 0;
    memcpy(&deleteMark, nodePage + 4 * sizeof(int), sizeof(int));
    
    //d=0;
    memcpy(&d, nodePage + 5 * sizeof(int), sizeof(int));
    
    //leftSibling = 0;
    memcpy(&leftSibling, nodePage + 6 * sizeof(int), sizeof(int));

    //rightSibling = 0;
    memcpy(&rightSibling, nodePage + 7 * sizeof(int), sizeof(int));
    
    int size, child;
    memcpy(&size, nodePage + 8 * sizeof(int), sizeof(int));
    for (int i = 0; i < size; i++){
        memcpy(&child, nodePage + (9 + i) * sizeof(int), sizeof(int));
        childList.push_back(child);
    }
    
    int maxKeyNum=2*d;
    int offset = (9 + maxKeyNum + 1) * sizeof(int);
    memcpy(&size, nodePage+ offset,sizeof(int));
    for(int i=0;i<size;i++){
        void *key;
        switch(attrType){
            case TypeVarChar:
                int varCharLen;
                memcpy(&varCharLen, nodePage + offset, sizeof(int));
                key = malloc(varCharLen + sizeof(int));
                memcpy(key, (char*)nodePage + offset, sizeof(int));
                memcpy(key, (char*)nodePage + offset+sizeof(int), varCharLen);
                offset += (varCharLen + sizeof(int));
                keys.push_back(key);
            case TypeInt:
                key = malloc(sizeof(int));
                memcpy(key, (char*)nodePage + offset, sizeof(int));
                offset += sizeof(int);
                keys.push_back(key);
            case TypeReal:
                key = malloc(sizeof(float));
                memcpy(key, (char*)nodePage + offset, sizeof(float));
                offset += sizeof(float);
                keys.push_back(key);
        }
    }
    return 0;
}

RC BtreeNode::compareKey(const void *key, const void *value, AttrType attrType){
	int result;
	switch(attrType){
		case TypeInt: {
			int kv = *(int*)key;
			int v = *(int*)value;
			if(kv>v) result=1;
			else if(kv==v) result=0;
			else result=-1;
			break;
		}
		case TypeReal: {
			float kv = *(float*)key;
			float v = *(float*)value;
			if(kv>v) result=1;
			else if(kv==v) result=0;
			else result=-1;
			break;
		}
        case TypeVarChar:
            break;
		// TODO: TypeVarChar compare
	}
	return result;
}

int BtreeNode::getKeyIndex(const void *key){
	if(keys.size()==0) return 0;
	int index, cp;
	for(index=0; index<keys.size(); index++){
		cp = compareKey(key, keys[index], attrType);
		if(cp>=0)
			break;
	}
	return index;
}

int BtreeNode::getChildIndex(const void *key, int keyIndex){

}

RC BtreeNode::insertIndex(const void *key, const int &childNodeID){
	int pos = getKeyIndex(key);

	keys.insert(keys.begin()+pos, (char*)key);
	childList.insert(childList.begin()+pos+1, childNodeID);
	return 0;
}


RC Btree::createNode(IXFileHandle &ixfileHandle, BtreeNode &node, NodeType nodeType){
	node.nodeType = nodeType;
	node.attrType = attrType;
	node.leftSibling = 0;
	node.rightSibling = 0;


	return 0;
}

RC Btree::readNode(IXFileHandle &ixfileHandle, int nodeID, BtreeNode &node){
	void *buffer = new char[PAGE_SIZE];
	memset(buffer, 0, PAGE_SIZE);
	RC rc = ixfileHandle.fileHandle.readPage(nodeID, buffer);
	node.initData(buffer);
	return rc;
}

