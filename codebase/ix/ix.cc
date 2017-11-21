
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
    if(!_pfm_manager)
        _pfm_manager=PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	RC rc=_pfm_manager->createFile(fileName);
    FileHandle fileHandle;
    char page_buffer[PAGE_SIZE];
    _pfm_manager->openFile(fileName.c_str(), fileHandle);
    memset(page_buffer, 0, PAGE_SIZE);
    fileHandle.appendPage(page_buffer);
    _pfm_manager->closeFile(fileHandle);
    return rc;
}

RC IndexManager::destroyFile(const string &fileName)
{
	return _pfm_manager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    if(ixfileHandle.fileHandle._isOpen())
        return -1;
    if(_pfm_manager->openFile(fileName,ixfileHandle.fileHandle)!=0)
        return -1;
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

RC IndexManager::readBtree(IXFileHandle &ixfileHandle, Btree *btree) const{
	char *buffer = new char[PAGE_SIZE];
	memset(buffer, 0, PAGE_SIZE);
	RC rc = ixfileHandle.fileHandle.readPage(0, buffer);
	ixfileHandle.ixReadPageCounter += 1;

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

RC IndexManager::writeBtree(IXFileHandle &ixfileHandle, const Btree *btree){
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
	ixfileHandle.ixWritePageCounter += 1;
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
    if(!ixfileHandle.fileHandle._isOpen())
        return -1;
    RC rc = readBtree(ixfileHandle, &ix_ScanIterator._btree);
    rc=ix_ScanIterator.initialization(ixfileHandle,attribute,lowKey,highKey,lowKeyInclusive,highKeyInclusive);
    return rc;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    Btree btree;
    readBtree(ixfileHandle, &btree);
    //btree.readNode(ixfileHandle,0,&node);
    recursivePrint(ixfileHandle,attribute,&btree, 0,btree.rootID);
    cout<<endl;
}

void IndexManager::recursivePrint(IXFileHandle &ixfileHandle, const Attribute &attribute,Btree* btree, int depth, int nodeID) const{
    BtreeNode node;
    //cout<<"NodeID: "<<nodeID<<endl;
    btree->readNode(ixfileHandle,nodeID,node);
    for(int i=0; i<depth; i++) printf("\t");

    int keySize=node.keys.size();
    //int _currKey=node.keys;
    //cout<<"keysize: "<<keySize<<endl;
    int maxKeyNum=2*node.d;
    int offset = (9 + maxKeyNum + 1) * sizeof(int);
    //memcpy(&keySize, node.nodePage+ offset,sizeof(int));
    
    offset+=sizeof(int);
    if(node.nodeType==Index){
        vector<int> links;
        int count=0;
        printf("\"keys\":[");
        while(count<keySize){
            if( count > 0 ) printf(",");
            printf("\"");
            switch(node.attrType){
                case TypeInt:
                    printf("%d",*((int*)node.keys[count]));
                    offset+=sizeof(int);
                    break;
                case TypeReal:
                    printf("%f",*((float*)node.keys[count]));
                    offset+=sizeof(int);
                    break;
                case TypeVarChar:
                    int varCharLen;
                    memcpy(&varCharLen,(char*)node.keys[count],sizeof(int));
                    //cout<<"varCharLen: "<<varCharLen<<endl;
                    //assert( varCharLen >= 0 && "something wrong with getting varchar key size\n");
                    void* key=malloc(varCharLen);
                    memset(key,0,varCharLen);
                    memcpy(key,(char*)node.keys[count]+sizeof(int),varCharLen);
                    //string sa( (char*)node.keys[count] , varCharLen);
                    cout<<(char*)key;
                    offset+=sizeof(int)+varCharLen;
                    free(key);
                    break;
            }
            printf("\"");
            links.push_back(node.childList[count]);
            count++;
        }
        cout<<"],"<<endl;
        links.push_back(node.childList[count]);// add the last link to vectory
        //cout<<"links.size(): "<<links.size()<<endl;
        for(int i=0;i<depth;i++)
            printf("\t");
        printf("\"children\":[");
        for( int i=0; i<links.size(); i++){
            recursivePrint(ixfileHandle,attribute,btree,depth+1,links[i]);
            if( i < links.size() - 1 ) cout<<",";
        }
        cout<<endl<<"]}"<<endl;
    }
    else if(node.nodeType==Leaf){
        int count=0;
        printf("\n\t{\"keys\":[");
        while(count<keySize){
            if( count > 0 ) printf(",");
            if((*(int*)node.keys[count]==300&&count==0)||*(int*)node.keys[count]==200) printf("\"");
            //print keys
            switch(node.attrType){
                case TypeInt:{
                    if((*(int*)node.keys[count]==300&&count==0)||*(int*)node.keys[count]==200)
                        cout<<*((int*)node.keys[count]);
                    offset+=sizeof(int);
                    break;
                }
                case TypeReal:
                    printf("%f",*((float*)node.keys[count]));
                    offset+=sizeof(int);
                    break;
                case TypeVarChar:
                    int varCharLen;
                    memcpy(&varCharLen,(char*)node.keys[count],sizeof(int));
                    //cout<<"varCharLen: "<<varCharLen<<endl;
                    //assert( varCharLen >= 0 && "something wrong with getting varchar key size\n");
                    void* key=malloc(varCharLen);
                    memset(key,0,varCharLen);
                    memcpy(key,(char*)node.keys[count]+sizeof(int),varCharLen);
                    //string sa( (char*)node.keys[count] , varCharLen);
                    cout<<(char*)key;
                    offset+=sizeof(int)+varCharLen;
                    free(key);
                    break;
            }
            if((*(int*)node.keys[count]==300&&count==0)||*(int*)node.keys[count]==200)
                printf(":[");
            //print RIDs
            //for(int i=0; i<node.buckets.size();i++){
                for(int i=0; i<node.buckets[count].size();i++){
                    cout<<"("<<node.buckets[count][i].pageNum<<","<<node.buckets[count][i].slotNum<<")";
                    if(i < node.buckets[count].size()-1&&count<node.buckets.size()-1){
                        //if(*(int*)node.keys[count]==300&&count==0)
                            printf(",");
                    }
                }
            //cout<<"keySize: "<<keySize<<endl;
            //cout<<"("<<node.buckets[count][0].pageNum<<","<<node.buckets[count][0].slotNum<<")";
            //}
            if((*(int*)node.keys[count]!=300&&count!=0)||*(int*)node.keys[count]==200)
                printf("]");
            count++;
        }
        printf("\"]}");

    }

}


IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::initialization(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive){
    _ixfileHandle=ixfileHandle;
    _ixfileHandle.fileHandle.getNumberOfPages();
    _attribute=attribute;
    _lowKey=lowKey;
    _highKey=highKey;
    _lowKeyInclusive=lowKeyInclusive;
    _highKeyInclusive=highKeyInclusive;
    _currNodeID=-1;
    return 0;
}


RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    if(_currNodeID==0)
        return IX_EOF;
    else if(_currNodeID==-1){
        if(_lowKey!=NULL){
            _currNodeID=_btree.findEntryPID(_ixfileHandle,_lowKey);
            _btree.readNode(_ixfileHandle,_currNodeID,_currNode);
            _currIndex=_currNode.getKeyIndex(_lowKey);
            if(!_lowKeyInclusive&&BtreeNode::compareKey(_currNode.keys[_currIndex],_lowKey,_btree.attrType)==0)
                    _currIndex++;
            _temp=_currIndex;
            _lastIndex=_currNode.keys.size();
        }
        else{
            _currNodeID=_btree.firstLeafID;
            _btree.readNode(_ixfileHandle,_currNodeID,_currNode);
            _currIndex=0;
            _temp=_currIndex;
            _lastIndex=_currNode.keys.size();
        }
    }
    else{
        _btree.readNode(_ixfileHandle,_currNodeID,_currNode);
        if(_temp==0)
            _lastIndex=_currNode.keys.size();
    }
    if(_currNodeID==_btree.rootID&&_currNode.keys.size()==0)//empty tree
        return IX_EOF;

    _currIndex=_temp-(_lastIndex-_currNode.keys.size());
    //cout<<"_currNode.buckets.size(): "<<_currNode.buckets.size()<<" _currIndex: "<<_currIndex<<" "<<" lastIndex: "<<_lastIndex<<endl;
    //cout<<"_currNodeID: "<<_currNodeID<<" _currIndex: "<<_currIndex<<"_currNode.keys[_currIndex]: "<<*(float*)_currNode.keys[_currIndex]<<" _highKey: "<<*(float*)_highKey<<" compareKey: "<<BtreeNode::compareKey(_currNode.keys[_currIndex],_highKey,_btree.attrType)<<endl;

    if(_highKey!=NULL&&BtreeNode::compareKey(_currNode.keys[_currIndex],_highKey,_btree.attrType)>=0){
        if(!_highKeyInclusive||(BtreeNode::compareKey(_currNode.keys[_currIndex],_highKey,_btree.attrType)>0))
            return IX_EOF;
    }

    rid=_currNode.buckets[_currIndex][0];//only consider the first RID
    switch(_btree.attrType){
        case TypeInt:
            memcpy(key,_currNode.keys[_currIndex],sizeof(int));
            break;
        case TypeReal:
            memcpy(key,_currNode.keys[_currIndex],sizeof(float));
            break;
        case TypeVarChar:
            int length;
            memcpy(&length,_currNode.keys[_currIndex],sizeof(int));
            memcpy(key,_currNode.keys[_currIndex],length+sizeof(int));
            break;
    }
    if(_currIndex<_currNode.keys.size()-1){
        _temp++;
    }
    else {
        _currIndex=0;
        _temp=_currIndex;
        _currNodeID=_currNode.rightSibling;
    }

    return 0;
    //Duplicate keys not considered yet.

}

RC IX_ScanIterator::close()
{
    //if(_lowKey)
        //delete (char*)_lowKey;
    //if(_highKey)
        //delete (char*)_highKey;
    return 0;
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

BtreeNode::BtreeNode(){
	memset(nodePage, 0, PAGE_SIZE);
	nodeID = 0;
	attrLen = 0;
	deleteMark = 0;
	leftSibling = 0;
	rightSibling = 0;
	d = 0;
	nodeType = Index;
	attrType = TypeInt;
}

void BtreeNode::getData(void *data){
    //nodeID=0;
    memset(nodePage, 0, PAGE_SIZE);
    keys.clear();
    childList.clear();

	memcpy(nodePage,(char*)data,PAGE_SIZE);

    memcpy(&nodeID, nodePage, sizeof(int));

    memcpy(&nodeType, nodePage + sizeof(int), sizeof(int));

    memcpy(&attrType, nodePage + 2 * sizeof(int), sizeof(int));

    memcpy(&attrLen, nodePage + 3 * sizeof(int), sizeof(int));

    memcpy(&deleteMark, nodePage + 4 * sizeof(int), sizeof(int));

    memcpy(&d, nodePage + 5 * sizeof(int), sizeof(int));

    memcpy(&leftSibling, nodePage + 6 * sizeof(int), sizeof(int));

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
    offset+=sizeof(int);
    for(int i=0;i<size;i++){
        switch(attrType){
            case TypeVarChar:{
                void *key;
                int varCharLen=0;
                memcpy(&varCharLen, nodePage + offset, sizeof(int));
                //cout<<"varCharLen: "<<varCharLen<<endl;
                key = malloc(varCharLen + sizeof(int));
                memset(key, 0, varCharLen+sizeof(int));
                memcpy(key, nodePage + offset, sizeof(int)+varCharLen);
                offset += (varCharLen + sizeof(int));
                keys.push_back(key);
                break;
            }
            case TypeInt:{
                void *key;
                key = malloc(sizeof(int));
                memcpy(key, nodePage + offset, sizeof(int));
                offset += sizeof(int);
                keys.push_back(key);
                break;
            }
            case TypeReal:{
                void *key;
                key = malloc(sizeof(float));
                memcpy(key, nodePage + offset, sizeof(float));
                offset += sizeof(float);
                keys.push_back(key);
                break;
            }
        }
    }
}

void BtreeNode::setData(BtreeNode *node){
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

    int size;
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
    offset+=sizeof(int);
    for(int i=0;i<size;i++){
        switch(node->attrType){
            case TypeVarChar:{
                //int varCharLen = *(int *)node->keys[i];
                int varCharLen;
                memcpy(&varCharLen,(int*)node->keys[i],sizeof(int));
                memcpy(buffer+offset, (char *)node->keys[i], sizeof(int)+varCharLen);
                offset += (varCharLen + sizeof(int));
                break;
            }
            case TypeInt:{
                memcpy(buffer+offset, (int*)node->keys[i], sizeof(int));
                offset += sizeof(int);
                break;
            }
            case TypeReal:{
                memcpy(buffer+offset, (float*)node->keys[i], sizeof(float));
                offset += sizeof(float);
                break;
            }
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
		case TypeVarChar: {
			int keyLen, valueLen;
			memcpy(&keyLen, (char*)key, sizeof(int));
            void *keyStr = malloc(keyLen);
            memset(keyStr, 0, keyLen);
            memcpy(keyStr, (char*)key+sizeof(int), keyLen);

			memcpy(&valueLen, (char*)value, sizeof(int));
            void *valueStr = malloc(valueLen);
            memset(valueStr, 0, valueLen);
            memcpy(valueStr, (char*)value+sizeof(int), valueLen);

			if(keyLen == valueLen && strncmp((char*)keyStr, (char*)valueStr, keyLen)==0) result=0;
			else if(strncmp((char*)keyStr, (char*)valueStr, keyLen)>0) result=1;
			else result=-1;
            free(keyStr);
            free(valueStr);
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
	if(keyIndex == keys.size())
		return keyIndex;
	if(compareKey(key, keys[keyIndex], attrType) >= 0)
		return keyIndex+1;
	else
		return keyIndex;
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
	//childList.insert(childList.begin()+index, 0);
	return 0;
}

RC BtreeNode::readEntry(IXFileHandle &ixfileHandle){
	RC rc = 0;
    for(int i=0;i<buckets.size();i++){
        buckets[i].clear();
    }
	buckets.clear();
	int offset, bucketSize;
	RID rid;
	switch(attrType){
		case TypeInt:
			offset = 11*sizeof(int) + 4*d * sizeof(int);
			break;
		case TypeReal:
			offset = 11*sizeof(float) + 4*d * sizeof(float);
			break;
		case TypeVarChar:
            offset = 11*sizeof(int) + 2 * attrLen * d + 4*d * sizeof(int);
			break;
	}
	memcpy(&bucketSize, nodePage+offset, sizeof(int));
	offset += sizeof(int);
	for(int i=0; i<bucketSize; i++){
		memcpy(&rid.pageNum, nodePage+offset, sizeof(unsigned));
		offset += sizeof(unsigned);
		memcpy(&rid.slotNum, nodePage+offset, sizeof(unsigned));
		offset += sizeof(unsigned);
		RidList ridList;
		ridList.push_back(rid);
		buckets.push_back(ridList);
	}
	return rc;
}

RC BtreeNode::writeEntry(IXFileHandle &ixfileHandle){
	RC rc = 0;
	int offset;
	switch(attrType){
		case TypeInt:
			offset = 11*sizeof(int) + 4*d * sizeof(int);
			break;
		case TypeReal:
			offset = 11*sizeof(float) + 4*d * sizeof(int);
			break;
		case TypeVarChar:
            offset = 11*sizeof(int) + 2 * attrLen * d + 4*d * sizeof(int);
			break;
	}
	memset(nodePage+offset, 0, PAGE_SIZE-offset);
	int bucketSize = buckets.size();
	memcpy(nodePage+offset, &bucketSize, sizeof(int));
	offset += sizeof(int);

	// debug info
	//cout << "check RID" << endl;
	//cout << "bucketSize: " << bucketSize << endl;
	for(int i=0; i<bucketSize; i++){
		/*
		if(i%50==0){
			cout << "buckets offset: " << offset << endl;
			cout << "nodeID: " << nodeID << endl;
			cout << "RID: pageNum: " << buckets[i][0].pageNum << " slotNum: " << buckets[i][0].slotNum << endl;
		}
		*/
		memcpy(nodePage+offset, &buckets[i][0].pageNum, sizeof(unsigned));
		offset += sizeof(unsigned);
		memcpy(nodePage+offset, &buckets[i][0].slotNum, sizeof(unsigned));
		offset += sizeof(unsigned);
	}
	//cout << "buckets offset: " << offset << endl;
	return rc;
}

Btree::Btree(){
	attrLen = 0;
	attrType = TypeInt;
	rootID = 0;
	d = 0;
    firstLeafID=1;
    //lastLeafID=1;
}

RC Btree::createNode(IXFileHandle &ixfileHandle, BtreeNode &node, NodeType nodeType){
	memset(node.nodePage, 0, PAGE_SIZE);
    node.nodeType = nodeType;
    node.attrType = attrType;
    node.attrLen = attrLen;
    node.deleteMark = 0;
    node.d = d;
    node.leftSibling = 0;
    node.rightSibling = 0;
	node.nodeID = ixfileHandle.fileHandle.getNumberOfPages();
	RC rc = ixfileHandle.fileHandle.appendPage(node.nodePage);
	ixfileHandle.ixAppendPageCounter += 1;
	return rc;
}

RC Btree::readNode(IXFileHandle &ixfileHandle, int nodeID, BtreeNode &node){
	char *buffer = new char[PAGE_SIZE];
	memset(buffer, 0, PAGE_SIZE);
	RC rc = ixfileHandle.fileHandle.readPage(nodeID, buffer);//Read Empty Page
	ixfileHandle.ixReadPageCounter += 1;
	node.getData(buffer);
	if(node.nodeType==Leaf)
		node.readEntry(ixfileHandle);
	delete[] buffer;
	return rc;
}

RC Btree::writeNode(IXFileHandle &ixfileHandle, BtreeNode &node){
	//cout << "writeNode nodeID: " << node.nodeID << endl;
	node.setData(&node);
	if(node.nodeType==Leaf)
		node.writeEntry(ixfileHandle);
	RC rc = ixfileHandle.fileHandle.writePage(node.nodeID, node.nodePage);
    ixfileHandle.ixWritePageCounter += 1;
	return rc;
}

RC Btree::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid){
	RC rc=0;
	if(rootID==0){
		// btree is empty
		attrType = attribute.type;
		attrLen = attribute.length;

		switch(attribute.type){
			case TypeInt:
				// keys, childList, RIDs
				d = (PAGE_SIZE / sizeof(int) - 12) / 8;
				break;
			case TypeReal:
				d = (PAGE_SIZE / sizeof(float) - 12) / 8;
				break;
			case TypeVarChar:
				// 2*d*(int+length(keys) + int(childList) + int(pageNum) + int(slotNum)) = PAGE_SIZE - metadata
				d = (PAGE_SIZE - 12 * sizeof(int)) / (2 * attribute.length + 8 * sizeof(int));
				break;
		}
        //cout << "d: " << d << endl;

		BtreeNode root;
		rc += createNode(ixfileHandle, root, Leaf);
        root.insertLeaf(key, rid);
        //cout << "keys.size(): " << root.keys.size() << endl;
		rc += writeNode(ixfileHandle, root);
		rootID = root.nodeID;
	}
	else{
		int splitNodeID = -1;
		bool split = false;
		void *copyUpKey;
        
		switch(attrType){
			case TypeInt:{
				copyUpKey = malloc(sizeof(int));
				break;
			}
			case TypeReal:{
				copyUpKey = malloc(sizeof(float));
				break;
			}
			case TypeVarChar:{
				copyUpKey = malloc(sizeof(int)+attrLen);
                memset(copyUpKey, 0, sizeof(int)+attrLen);
				break;
			}
		}

		//cout << "insert rootID: " << rootID << endl;
		recursiveInsert(ixfileHandle, key, rid, rootID, splitNodeID, copyUpKey, split);
		free(copyUpKey);
	}
	return rc;
}

RC Btree::recursiveInsert(IXFileHandle &ixfileHandle, const void *key, const RID &rid, int nodeID, int &splitNodeID, void *copyUpKey, bool &split){
	BtreeNode node;
	RC rc=0;
	int index, childIndex, childID;
	rc += readNode(ixfileHandle, nodeID, node);

	if(node.nodeType==Leaf){
		// debug info

		//cout << "check node" << endl;
		//cout << "nodeID: " << node.nodeID << endl;
		//cout << "keys.size(): " << node.keys.size() << endl;
		//cout << "nodeType: " << node.nodeType << endl;
		//for(int i=0; i<node.keys.size(); i++){
		//	cout << "node.key: " << *(int*) node.keys[i] << endl;
		//}


		if(node.keys.size()==2*d){
			// node is full, need to split
			//cout << "full nodeID: " << nodeID << endl;
			BtreeNode newNode;
			rc += createNode(ixfileHandle, newNode, Leaf);
			rc += splitNode(ixfileHandle, node, newNode, copyUpKey);


			int index = node.getKeyIndex(key);
			if(index < d){
				node.insertLeaf(key, rid);
			}
			else{
				newNode.insertLeaf(key, rid);
			}

			rc += writeNode(ixfileHandle, node);
			rc += writeNode(ixfileHandle, newNode);
			split = true;
			splitNodeID = newNode.nodeID;

			// debug info
			/*
			cout << "check split" << endl;
			cout << "oldNodeID: " << node.nodeID << endl;
			cout << "newNodeID: " << newNode.nodeID << endl;
			cout << "oldNode first key: " << *(int*)node.keys[0] << " last key: " << *(int*)node.keys[node.keys.size()-1] << endl;
			cout << "newNode first key: " << *(int*)newNode.keys[0] << " last key: " << *(int*)newNode.keys[newNode.keys.size()-1] << endl;
			*/
			// get copyUpKey
			switch(newNode.attrType){
				case TypeInt: {
					memcpy((char*)copyUpKey, (char*)newNode.keys[0], sizeof(int));
					break;
				}
				case TypeReal: {
					memcpy((char*)copyUpKey, (char*)newNode.keys[0], sizeof(float));
					break;
				}
				case TypeVarChar: {
					int varCharLen = 0;
					memcpy(&varCharLen, (char*)newNode.keys[0], sizeof(int));
                    copyUpKey = malloc(sizeof(int)+varCharLen);
                    memset(copyUpKey, 0, sizeof(int)+varCharLen);
					memcpy((char*)copyUpKey, (char*)newNode.keys[0], sizeof(int)+varCharLen);

                    // debug info
					/*
                    cout << "check leaf copyUpKey varchar " << endl;
                    cout << "varCharLen: " << varCharLen << endl;
                    void *charKey = malloc(varCharLen+1);
                    memset(charKey, 0, varCharLen+1);
                    memcpy((char*)charKey, (char*)copyUpKey+sizeof(int), varCharLen);
                    cout << "charKey: " << (char*)charKey << endl;
                    free(charKey);
                    */

					break;
				}
			}

			//cout << "copyUpKey: " << *(char*)copyUpKey << endl;

			// if node is rootNode, then we need to create new root
			if(node.nodeID == rootID){
				//cout << "special case!" << endl;
				BtreeNode rootNode;
				rc += createNode(ixfileHandle, rootNode, Index);
				// update rootID
				rootID = rootNode.nodeID;
				// update rootNode
				//cout << "insert first root key: " << *(int*)copyUpKey << endl;
				rootNode.keys.push_back(copyUpKey);
				rootNode.childList.push_back(node.nodeID);
				rootNode.childList.push_back(newNode.nodeID);
				rc += writeNode(ixfileHandle, rootNode);
				split = false;
				//cout << "create new root node!" << endl;
			}
		}
		else{
			node.insertLeaf(key, rid);
			rc += writeNode(ixfileHandle, node);
		}
	}
	else{ // nodeType = Index
		index = node.getKeyIndex(key);
		childIndex = node.getChildIndex(key, index);
		childID = node.childList[childIndex];
        // debug info
        /*
        cout << "check split index node" << endl;
        cout << "insert indexNode index: " << index << endl;
        cout << "node.keys.size(): " << node.keys.size() << endl;
        cout << "getChildIndex: " << childIndex << endl;
        cout << "childID: " << childID << endl;
        for(int i=0; i<node.childList.size(); i++){
            cout << "ChildList ID: " << node.childList[i] << endl;
        }
        */
        
		recursiveInsert(ixfileHandle, key, rid, childID, splitNodeID, copyUpKey, split);

		if(split){
			if(node.keys.size()==2*d){
                // node is full, need to split
                BtreeNode newNode;
                rc += createNode(ixfileHandle, newNode, Index);
                node.insertIndex(copyUpKey, splitNodeID);
                rc += splitNode(ixfileHandle, node, newNode, copyUpKey);

                rc += writeNode(ixfileHandle, node);
                rc += writeNode(ixfileHandle, newNode);

                splitNodeID = newNode.nodeID;
                //split = true;
                // debug info
                /*
                cout << "check split" << endl;
                cout << "oldNodeID: " << node.nodeID << endl;
                cout << "newNodeID: " << newNode.nodeID << endl;
                cout << "oldNode first key: " << *(int*)node.keys[0] << " last key: " << *(int*)node.keys[node.keys.size()-1] << endl;
                cout << "newNode first key: " << *(int*)newNode.keys[0] << " last key: " << *(int*)newNode.keys[newNode.keys.size()-1] << endl;
				*/

                if(node.nodeID == rootID){
                    BtreeNode rootNode;
                    rc += createNode(ixfileHandle, rootNode, Index);
                    // update rootID
                    rootID = rootNode.nodeID;
                    // update rootNode
                    rootNode.keys.push_back(copyUpKey);
                    rootNode.childList.push_back(node.nodeID);
                    rootNode.childList.push_back(newNode.nodeID);
                    rc += writeNode(ixfileHandle, rootNode);
                    splitNodeID = -1;
                    split = false;
                }

            }
			else{
				node.insertIndex(copyUpKey, splitNodeID);
				rc += writeNode(ixfileHandle, node);
				split = false;
			}
		}
	}
	return rc;
}

RC Btree::splitNode(IXFileHandle &ixfileHandle, BtreeNode &oldNode, BtreeNode &newNode, void* copyUpKey){
	RC rc = 0;

	if(oldNode.nodeType==Leaf){
		// debug info
		/*
		cout << "split info" << endl;
		cout << "oldNodeID: " << oldNode.nodeID << endl;
		cout << "newNodeID: " << newNode.nodeID << endl;
		cout << "keys.size: " << oldNode.keys.size() << endl;
		cout << "buckets size: " << oldNode.buckets.size() << endl;
		*/
		for(int i=0; i<d; i++){
			// move last half keys from oldNode to newNode
			newNode.keys.push_back(oldNode.keys[d+i]);
			newNode.buckets.push_back(oldNode.buckets[d+i]);
		}
		// remove last half keys in oldNode
		oldNode.keys.erase(oldNode.keys.begin()+d, oldNode.keys.end());
		oldNode.buckets.erase(oldNode.buckets.begin()+d, oldNode.buckets.end());
		// update sibling
		if(oldNode.rightSibling != 0){
			BtreeNode originNextOldNode;
			rc += readNode(ixfileHandle, oldNode.rightSibling, originNextOldNode);
			originNextOldNode.leftSibling = newNode.nodeID;
			rc += writeNode(ixfileHandle, originNextOldNode);
		}
		newNode.rightSibling = oldNode.rightSibling;
		oldNode.rightSibling = newNode.nodeID;
		newNode.leftSibling = oldNode.nodeID;
		// debug info
		/*
		cout << "check sibling" << endl;
		cout << "oldNode left: " << oldNode.leftSibling << " right: " << oldNode.rightSibling << endl;
		cout << "newNode left: " << newNode.leftSibling << " right: " << newNode.rightSibling << endl;
		*/

		return rc;
	}
	else{ // node is Index
        // debug info
		/*
        cout << "split index info" << endl;
        cout << "oldNodeID: " << oldNode.nodeID << endl;
        cout << "newNodeID: " << newNode.nodeID << endl;
        cout << "keys.size: " << oldNode.keys.size() << endl;
		*/
        for(int i=0; i<d; i++){
            // move last half keys from oldNode to newNode
            newNode.keys.push_back(oldNode.keys[d+i+1]);
            newNode.childList.push_back(oldNode.childList[d+i+1]);
        }
        newNode.childList.push_back(oldNode.childList[2 * d + 1]);

        // get copyUpKey
        switch(newNode.attrType){
            case TypeInt: {
                memcpy((char*)copyUpKey, (char*)oldNode.keys[d], sizeof(int));
                //cout << "indexNode copyUpKey: " << *((int*)copyUpKey) << endl;
                break;
            }
            case TypeReal: {
                memcpy((char*)copyUpKey, (char*)oldNode.keys[d], sizeof(float));
                //cout << "copyUpKey: " << *((float*)copyUpKey) << endl;
                break;
            }
            case TypeVarChar: {
                int varCharLen=0;
                memcpy(&varCharLen, (char*)oldNode.keys[d], sizeof(int));
                copyUpKey = malloc(sizeof(int)+varCharLen);
                memset(copyUpKey, 0, sizeof(int)+varCharLen);
                memcpy((char*)copyUpKey, (char*)oldNode.keys[d], sizeof(int)+varCharLen);

                // debug info
                /*
                cout << "check split index node varchar" << endl;
                cout << "varCharLen: " << varCharLen << endl;
                void *charKey = malloc(varCharLen+1);
                memset(charKey, 0, varCharLen+1);
                memcpy((char*)charKey, (char*)copyUpKey+sizeof(int), varCharLen);
                cout << "charKey: " << (char*)charKey << endl;
                free(charKey);
                */
                

                break;
            }
        }

        // remove last half keys in oldNode
        oldNode.keys.erase(oldNode.keys.begin()+d, oldNode.keys.end());
        oldNode.childList.erase(oldNode.childList.begin()+d+1, oldNode.childList.end());

		return rc;
	}
}

RC Btree::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid){
	RC rc=0;
    if(rootID==0){
		return -1;
    }
	int nodeID = findEntryPID(ixfileHandle, key);
   //if(nodeID==rootID)

	BtreeNode node;
	rc += readNode(ixfileHandle, nodeID, node);
    if(node.deleteMark==1)//node been deleted
        return -1;
    
    int index=node.getKeyIndex(key);
    //cout<<"first stored key: "<<*(int*)node.keys[0]<<" Stored key: "<<*(int*)node.keys[index]<<" key: "<<*(int*)key<<endl;
    // check index 
    if(index == node.keys.size() || node.compareKey(key, node.keys[index], attrType)!=0){
        return -1;
    }

    //lazy delete
    for(int i=0; i<node.buckets[index].size(); i++){
        if(node.buckets[index][i].pageNum == rid.pageNum && node.buckets[index][i].slotNum == rid.slotNum){
            node.buckets[index].erase(node.buckets[index].begin()+i);
        }
    }

    if(node.buckets[index].size() == 0){
        node.keys.erase(node.keys.begin()+index);
        node.buckets.erase(node.buckets.begin()+index);
    }
    //node.buckets.erase(node.buckets[index].begin());

    // node is deleted
    if(node.keys.size() == 0){
        node.deleteMark = 1;
    }

	rc += writeNode(ixfileHandle, node);
	return rc;
}

int Btree::findEntryPID(IXFileHandle &ixfileHandle, const void *key){
	return recursiveFind(ixfileHandle, key, rootID);
}

int Btree::recursiveFind(IXFileHandle &ixfileHandle, const void *key, int nodeID){
	BtreeNode node;
	readNode(ixfileHandle, nodeID, node);
	int index, childIndex, childID;

	if(node.nodeType==Leaf){
		//cout << "check leafNode find: " << node.nodeID << endl;
		return node.nodeID;
	}
	else{
		index = node.getKeyIndex(key);
		childIndex = node.getChildIndex(key, index);
		childID = node.childList[childIndex];
		// debug info

		//cout << "check find" << endl;
		//cout << "index: " << index << endl;
		//cout << "childIndex: " << childIndex << endl;
		//cout << "childID: " << childID << endl;

		return recursiveFind(ixfileHandle, key, childID);
	}
}

