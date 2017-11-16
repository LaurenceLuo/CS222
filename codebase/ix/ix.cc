
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
	PagedFileManager *_pfm_manager=PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	return _pfm_manager->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName)
{
	return _pfm_manager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    if(ixfileHandle.fileHandle._isOpen())
        return -1;
    if(_pfm_manager->openFile(fileName,ixfileHandle.fileHandle)!=0){
        cout<< "File " << fileName << " open fail!" << endl << endl;
        return -1;
    }
    ixfileHandle.fileHandle.getNumberOfPages();
    return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	return _pfm_manager->closeFile(ixfileHandle.fileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	RC rc = 0;
	Btree btree;
	rc += readBtree(ixfileHandle, &btree);
	rc += btree.insertEntry(ixfileHandle, attribute, key, rid);
	rc += writeBtree(ixfileHandle, &btree);
    return rc;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	RC rc = 0;
	Btree btree;
	rc += readBtree(ixfileHandle, &btree);
	rc += btree.deleteEntry(ixfileHandle, attribute, key, rid);
	rc += writeBtree(ixfileHandle, &btree);
    return rc;
}

RC readBtree(IXFileHandle &ixfileHandle, Btree *btree){
	char *buffer = new char[PAGE_SIZE];
	memset(buffer, 0, PAGE_SIZE);
	RC rc = ixfileHandle.fileHandle.readPage(0, buffer);

	int offset = 0;
    memcpy(&btree->rootID, (char*) buffer + offset, sizeof(int));
    offset += sizeof(int);
    memcpy(&btree->attrType, (char*) buffer + offset, sizeof(int));
    offset += sizeof(int);
    memcpy(&btree->d, (char*) buffer + offset, sizeof(int));
    offset += sizeof(int);
    memcpy(&btree->attrLen, (char*) buffer + offset, sizeof(int));

    delete[] buffer;
    return rc;
}

RC writeBtree(IXFileHandle &ixfileHandle, const Btree *btree){
	char *buffer = new char[PAGE_SIZE];
	memset(buffer, 0, PAGE_SIZE);

	int offset = 0;
	memcpy(buffer, &btree->rootID, sizeof(int));
	offset += sizeof(int);
	memcpy(buffer+offset, &btree->attrType, sizeof(int));
	offset += sizeof(int);
	memcpy(buffer+offset, &btree->d, sizeof(int));
	offset += sizeof(int);
	memcpy(buffer+offset, &btree->attrLen, sizeof(int));

	RC rc = ixfileHandle.fileHandle.writePage(0, buffer);
	delete[] buffer;
	return rc;
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

void BtreeNode::getData(void *data){
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
                break;

            case TypeInt:
                key = malloc(sizeof(int));
                memcpy(key, (char*)nodePage + offset, sizeof(int));
                offset += sizeof(int);
                keys.push_back(key);
                break;

            case TypeReal:
                key = malloc(sizeof(float));
                memcpy(key, (char*)nodePage + offset, sizeof(float));
                offset += sizeof(float);
                keys.push_back(key);
                break;
        }
    }
}

void setData(BtreeNode *node){
	char *buffer = new char[PAGE_SIZE];
	memset(buffer, 0, PAGE_SIZE);
	int offset = 0;

    memcpy(buffer, &node->nodeID, sizeof(int));
	offset += sizeof(int);

    memcpy(buffer+offset, &node->nodeType, sizeof(int));
    offset += sizeof(int);

    memcpy(buffer+offset, &node->attrType, sizeof(int));
    offset += sizeof(int);

    memcpy(buffer+offset, &node->attrLen, sizeof(int));
    offset += sizeof(int);

    memcpy(buffer+offset, &node->deleteMark, sizeof(int));
    offset += sizeof(int);

    memcpy(buffer+offset, &node->d, sizeof(int));
    offset += sizeof(int);

    memcpy(buffer+offset, &node->leftSibling, sizeof(int));
    offset += sizeof(int);

    memcpy(buffer+offset, &node->rightSibling, sizeof(int));
    offset += sizeof(int);

    int size, child;
    size = node->childList.size();
    memcpy(buffer+offset, &size, sizeof(int));
    offset += sizeof(int);

    for (int i = 0; i < size; i++){
        memcpy(buffer+offset, &node->childList[i], sizeof(int));
        offset += sizeof(int);
    }

    int maxKeyNum=2*node->d;
    size = node->keys.size();
    offset = (9 + maxKeyNum + 1) * sizeof(int);
    memcpy(buffer+offset, &size, sizeof(int));
    for(int i=0;i<size;i++){
        void *key;
        switch(node->attrType){
            case TypeVarChar:
                int varCharLen = *(int *)node->keys[i];
                memcpy(buffer+offset, (char *)node->keys[i], sizeof(int)+varCharLen);
                offset += (varCharLen + sizeof(int));
                break;

            case TypeInt:
                memcpy(buffer+offset, &node->keys[i], sizeof(int));
                offset += sizeof(int);
                break;

            case TypeReal:
            		memcpy(buffer+offset, &node->keys[i], sizeof(float));
                offset += sizeof(float);
                break;
        }
    }
    memcpy(node->nodePage, buffer, PAGE_SIZE);
    delete[] buffer;
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
		// TODO: TypeVarChar compare
		case TypeVarChar: {
			break;
		}
	}
	return result;
}

int BtreeNode::getKeyIndex(const void *key){
	int index=0;
	for(index=0; index<keys.size(); index++){
		if(compareKey(key, keys[index], attrType) <= 0)
			break;
	}
	return index;
}

int BtreeNode::getChildIndex(const void *key, int keyIndex){
	if(compareKey(key, keys[keyIndex], attrType) >= 0)
		return keyIndex;
	else
		return keyIndex+1;
}

RC BtreeNode::insertIndex(const void *key, const int &childNodeID){
	int index = getKeyIndex(key);

	keys.insert(keys.begin()+index, (char*)key);
	childList.insert(childList.begin()+index+1, childNodeID);
	return 0;
}

RC BtreeNode::insertLeaf(const void *key, const RID &rid){
	int index = getKeyIndex(key);
	keys.insert(keys.begin()+index, (char*)key);
	RidList ridList;
	ridList.push_back(rid);
	buckets.insert(buckets.begin()+index, ridList);
	return 0;
}

RC BtreeNode::readEntry(IXFileHandle &ixfileHandle){

}

RC BtreeNode::writeEntry(IXFileHandle &ixfileHandle){

}

RC Btree::createNode(IXFileHandle &ixfileHandle, BtreeNode &node, NodeType nodeType){
	memset(node.nodePage, 0, PAGE_SIZE);
	node.nodeType = nodeType;
	node.nodeID = ixfileHandle.fileHandle.getNumberOfPages();
	RC rc = ixfileHandle.fileHandle.appendPage(node.nodePage);
	return rc;
}

RC Btree::readNode(IXFileHandle &ixfileHandle, int nodeID, BtreeNode &node){
	char *buffer = new char[PAGE_SIZE];
	memset(buffer, 0, PAGE_SIZE);
	RC rc = ixfileHandle.fileHandle.readPage(nodeID, buffer);
	node.getData(buffer);
	delete[] buffer;
	return rc;
}

RC Btree::writeNode(IXFileHandle &ixfileHandle, BtreeNode &node){

}

RC Btree::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid){
	// btree is empty
	RC rc=0;
	if(not rootID){
		attrType = attribute.type;
		attrLen = attribute.length;
		// get order d
		switch(attribute.type){
			case TypeInt:
				d = (PAGE_SIZE / sizeof(int) - 8) / 8;
				break;
			case TypeReal:
				d = (PAGE_SIZE / sizeof(float) - 8) / 8;
				break;
			// TODO
			case TypeVarChar:
				break;
		}

		BtreeNode root;
		rc += createNode(ixfileHandle, root, Leaf);
		root.insertLeaf(key, rid);
		rc += writeNode(ixfileHandle, root);
		rootID = root.nodeID;
	}
	else{
		recursiveInsert(ixfileHandle, key, rid);
	}
	return rc;
}

RC Btree::recursiveInsert(IXFileHandle &ixfileHandle, const void *key, const RID &rid){
	return 0;
}

RC Btree::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid){

}

int Btree::findEntry(IXFileHandle &ixfileHandle, const void *key){

}

