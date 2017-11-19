
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
}

void IndexManager::recursivePrint(IXFileHandle &ixfileHandle, const Attribute &attribute,Btree* btree, int depth, int nodeID) const{
    BtreeNode node;
    btree->readNode(ixfileHandle,nodeID,node);
    for(int i=0; i<depth; i++) printf("\t");

    int keySize;
    int maxKeyNum=2*node.d;
    int offset = (9 + maxKeyNum + 1) * sizeof(int);
    memcpy(&keySize, node.nodePage+ offset,sizeof(int));
    offset+=sizeof(int);
    if(node.nodeType==Index){
        vector<int> links;
        int count=0;
        printf("{\"keys\":[");
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
                    memcpy(&varCharLen,node.nodePage+offset,sizeof(int));
                    //assert( varCharLen >= 0 && "something wrong with getting varchar key size\n");
                    string sa( (char*)node.keys[count] , varCharLen);
                    printf("%s",sa.c_str());
                    offset+=sizeof(int)+varCharLen;
                    break;
            }
            printf("\"");
            links.push_back(node.childList[count]);
            count++;
        }
        printf("],\n");
        links.push_back(node.childList[count+1]);// add the last link to vectory

        for(int i=0;i<depth;i++)
            printf("\t");
        printf("\"children\":[");
        for( int i=0; i<links.size(); i++){
            recursivePrint(ixfileHandle,attribute,btree,depth+1,links[i]);
            if( i < links.size() - 1 ) printf(",\n");
        }
        printf("]}\n");
    }
    else if(node.nodeType==Leaf){
        int count=0;
        printf("{\"keys\":[");
        while(count<keySize){
            if( count > 0 ) printf(",");
            printf("\"");
            //print keys
            switch(node.attrType){
                case TypeInt:
                    cout<<*((int*)node.keys[count]);
                    offset+=sizeof(int);
                    break;
                case TypeReal:
                    printf("%f",*((float*)node.keys[count]));
                    offset+=sizeof(int);
                    break;
                case TypeVarChar:
                    int varCharLen;
                    memcpy(&varCharLen,node.nodePage+offset,sizeof(int));
                    //assert( varCharLen >= 0 && "something wrong with getting varchar key size\n");
                    string sa( (char*)node.keys[count] , varCharLen);
                    printf("%s",sa.c_str());
                    offset+=sizeof(int)+varCharLen;
                    break;
            }
            printf(":[");
            //print RIDs
            for(int i=0; i<node.buckets.size();i++){
                for(int j=0; j<node.buckets[i].size();j++){
                    cout<<"("<<node.buckets[i][j].pageNum<<","<<node.buckets[i][j].slotNum<<")";
                    if(i < node.buckets.size()-1&&j<node.buckets[i].size()-1) printf(",");
                }
            }
            printf("]\"");
            count++;
        }
        printf("]}\n");

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
    int smallerThanLK=1;//=1 means key>lowKey
    int largerThanHK=-1;//=-1 means key<highKey
    if(_lowKey!=NULL){
        smallerThanLK=BtreeNode::compareKey(key,_lowKey,_btree.attrType);
        if(smallerThanLK<0||(smallerThanLK==0&&!_lowKeyInclusive))
            return IX_EOF;
    }
    if(_highKey!=NULL){
        largerThanHK=BtreeNode::compareKey(key,_highKey,_btree.attrType);
        if(largerThanHK>0||(largerThanHK==0&&!_highKeyInclusive))
            return IX_EOF;
    }
    if(_currNodeID==0)
        return IX_EOF;
    else if(_currNodeID==-1){
        if(_lowKey!=NULL){
            _currNodeID=_btree.findEntryPID(_ixfileHandle,_lowKey);
            _btree.readNode(_ixfileHandle,_currNodeID,_currNode);
            _currIndex=_currNode.getKeyIndex(_lowKey);
        }
        else{
            _currNodeID=_btree.firstLeafID;
            _btree.readNode(_ixfileHandle,_currNodeID,_currNode);
            _currIndex=0;
        }
    }
    else{
        _btree.readNode(_ixfileHandle,_currNodeID,_currNode);
    }

    if(_highKey!=NULL&&BtreeNode::compareKey(_currNode.keys[_currIndex],_highKey,_btree.attrType)>0){
        return IX_EOF;
    }
    //DeleteMark not considered yet.
    rid=_currNode.buckets[_currIndex][0];//only consider the first RID

    if(_currIndex<_currNode.keys.size()-1){
        _currIndex++;
    }
    else {
        _currIndex=0;
        _currNodeID=_currNode.rightSibling;
    }

    return 0;
    //Duplicate keys not considered yet.

}

RC IX_ScanIterator::close()
{
    if((char*)_lowKey)
        free((char*)_lowKey);
    if((char*)_highKey)
        free((char*)_highKey);
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
                int varCharLen;
                memcpy(&varCharLen, nodePage + offset, sizeof(int));
                key = malloc(varCharLen + sizeof(int));
                memcpy(key, nodePage + offset, sizeof(int));
                memcpy(key, nodePage + offset+sizeof(int), varCharLen);
                offset += (varCharLen + sizeof(int));
                keys.push_back(key);
                //free(key);
                break;
            }
            case TypeInt:{
                void *key;
                key = malloc(sizeof(int));
                memcpy(key, nodePage + offset, sizeof(int));
                offset += sizeof(int);
                keys.push_back(key);
                //free(key);
                break;
            }
            case TypeReal:{
                void *key;
                key = malloc(sizeof(float));
                memcpy(key, nodePage + offset, sizeof(float));
                offset += sizeof(float);
                keys.push_back(key);
                //free(key);
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
                int varCharLen = *(int *)node->keys[i];
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
			char *keyStr = new char[keyLen];
			memcpy(&keyStr, (char*)key+sizeof(int), keyLen);

			memcpy(&valueLen, (char*)value, sizeof(int));
			char *valueStr = new char[valueLen];
			memcpy(&valueStr, (char*)value+sizeof(int), valueLen);

			if(strcmp(keyStr, valueStr)==0) result=0;
			else if(strcmp(keyStr, valueStr)>0) result=1;
			else result=-1;
			delete[] keyStr;
			delete[] valueStr;
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
	if(compareKey(key, keys[keyIndex], attrType) == 0)
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
		// TODO
		case TypeVarChar:
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
		// TODO
		case TypeVarChar:
			break;
	}
	int bucketSize = buckets.size();
	memcpy(nodePage+offset, &bucketSize, sizeof(int));
	offset += sizeof(int);

	for(int i=0; i<bucketSize; i++){
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
    lastLeafID=1;
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
	node.setData(&node);
	if(node.nodeType==Leaf)
		node.writeEntry(ixfileHandle);
	cout << "writeNode nodeID: " << node.nodeID << endl;
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
				d = (PAGE_SIZE / sizeof(int) - 11) / 8;
				break;
			case TypeReal:
				d = (PAGE_SIZE / sizeof(float) - 11) / 8;
				break;
			// TODO
			case TypeVarChar:
				//d = (PAGE_SIZE - sizeof(int)*11) / (2*(3*sizeof(int)+attribute.length));
				break;
		}

		BtreeNode root;
		rc += createNode(ixfileHandle, root, Leaf);
        root.insertLeaf(key, rid);
		rc += writeNode(ixfileHandle, root);
		rootID = root.nodeID;
	}
	else{
		int parentID = -1;
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
				break;
			}
		}
		//cout << "insert rootID: " << rootID << endl;
		recursiveInsert(ixfileHandle, key, rid, rootID, parentID, copyUpKey, split);
		free(copyUpKey);
	}
	return rc;
}

RC Btree::recursiveInsert(IXFileHandle &ixfileHandle, const void *key, const RID &rid, int nodeID, int &parentID, void *copyUpKey, bool &split){
	BtreeNode node;
	RC rc=0;
	int index, childIndex, childID;
	rc += readNode(ixfileHandle, nodeID, node);
	//cout << "nodeType: " << node.nodeType << endl;

	if(node.nodeType==Leaf){
		//cout << "keys.size(): " << node.keys.size() << endl;
		//cout << "d: " << d << endl;
		if(node.keys.size()==2*d){
			// node is full, need to split
			cout << "full nodeID: " << nodeID << endl;
			BtreeNode newNode;
			rc += createNode(ixfileHandle, newNode, Leaf);
			rc += splitNode(ixfileHandle, node, newNode, nodeID, copyUpKey);
			//cout << "split suc" << endl;

			int index = node.getKeyIndex(key);
			if(index > d){
				newNode.insertLeaf(key, rid);
			}
			else{
				node.insertLeaf(key, rid);
			}

			rc += writeNode(ixfileHandle, node);
			rc += writeNode(ixfileHandle, newNode);
			split = true;
			// TODO
			parentID = newNode.nodeID;

			// get copyUpKey
			switch(newNode.attrType){
				case TypeInt: {
					memcpy(&copyUpKey, &newNode.keys[0], sizeof(int));
					break;
				}
				case TypeReal: {
					memcpy(&copyUpKey, &newNode.keys[0], sizeof(float));
					break;
				}
				case TypeVarChar: {
					int varCharLen;
					memcpy(&varCharLen, &newNode.keys[0], sizeof(int));
					memcpy(&copyUpKey, &newNode.keys[0], sizeof(int)+varCharLen);
					break;
				}
			}
			cout << "copyUpKey: " << *((int*)copyUpKey) << endl;

			// if node is rootNode, then we need to create new root
			if(node.nodeID == rootID){
				cout << "special case!" << endl;
				BtreeNode rootNode;
				rc += createNode(ixfileHandle, rootNode, Index);
				// update rootID
				rootID = rootNode.nodeID;
				// update rootNode
				rootNode.keys.push_back(copyUpKey);
				rootNode.childList.push_back(node.nodeID);
				rootNode.childList.push_back(newNode.nodeID);
				rc += writeNode(ixfileHandle, rootNode);
				split = false;
				cout << "create new root node!" << endl;
			}
		}
		else{
			node.insertLeaf(key, rid);
			rc += writeNode(ixfileHandle, node);
		}
	}
	else{ // nodeType = Index
		index = node.getKeyIndex(key);
		//cout << "insert indexNode index: " << index << endl;
		//cout << "node.keys.size(): " << *(int*)node.keys[0] << endl;
		childIndex = node.getChildIndex(key, index);
		//cout << "getChildIndex: " << childIndex << endl;
		childID = node.childList[childIndex];
		//cout << "childID: " << childID << endl;
		// debug info
		/*
		for(int i=0; i<node.childList.size(); i++){
			cout << "ChildList ID: " << node.childList[i] << endl;
		}
		*/
		//
		recursiveInsert(ixfileHandle, key, rid, childID, parentID, copyUpKey, split);

		if(split){
			if(node.keys.size()==2*d){
				// node is full, need to split
				BtreeNode newNode;
				rc += createNode(ixfileHandle, newNode, Index);

				rc += splitNode(ixfileHandle, node, newNode, nodeID, copyUpKey);

				int index = node.getKeyIndex(key);


				rc += writeNode(ixfileHandle, node);
				rc += writeNode(ixfileHandle, newNode);

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
					split = false;
				}

			}
			else{
				node.insertIndex(key, parentID);
				rc += writeNode(ixfileHandle, node);
				split = false;
			}
		}
	}
	return rc;
}

RC Btree::splitNode(IXFileHandle &ixfileHandle, BtreeNode &oldNode, BtreeNode &newNode, int parentID, const void* copyUpKey){
	RC rc = 0;

	if(oldNode.nodeType==Leaf){
		//cout << "keys.size: " << oldNode.keys.size() << endl;
		//cout << "buckets size: " << oldNode.buckets.size() << endl;
		for(int i=0; i<d; i++){
			// move last half keys from oldNode to newNode
			//cout << "index: " << d+i << endl;
			newNode.keys.push_back(oldNode.keys[d+i]);
			newNode.buckets.push_back(oldNode.buckets[d+i]);
		}
		//cout << "newNode data get" << endl;
		// remove last half keys in oldNode
		oldNode.keys.erase(oldNode.keys.begin()+d, oldNode.keys.end());
		oldNode.buckets.erase(oldNode.buckets.begin()+d, oldNode.buckets.end());
		//cout << "oldNode erase data" << endl;

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
		return rc;
	}
	else{ // node is Index
		for(int i=0; i<d; i++){
			// move last half keys from oldNode to newNode
			newNode.keys.push_back(oldNode.keys[d+i]);
			newNode.childList.push_back(oldNode.childList[d+i]);
		}
		// remove last half keys in oldNode
		oldNode.keys.erase(oldNode.keys.begin()+d, oldNode.keys.end());
		oldNode.childList.erase(oldNode.childList.begin()+d, oldNode.childList.end());

		return rc;
	}
}

RC Btree::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid){
	RC rc=0;
	if(rootID==0)
		return -1;
	int nodeID = findEntryPID(ixfileHandle, key);
	BtreeNode node;
	rc += readNode(ixfileHandle, nodeID, node);
    //lazy delete
	if(node.deleteMark==1)
		return -1;
	else
        node.deleteMark=1;
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
		return node.nodeID;
	}
	else{
		index = node.getKeyIndex(key);
		childIndex = node.getChildIndex(key, index);
		childID = node.childList[childIndex];
		return recursiveFind(ixfileHandle, key, childID);
	}
}

