
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
	this->input = input;
	this->condition = condition;
	getAttributes(attrs);
	value = malloc(sizeof(int));
}

// ... the rest of your implementations go here

Project::Project(Iterator *input, const vector<string> &attrNames){
	this->input = input;
	this->attrNames = attrNames;
	input->getAttributes(allAttrs);
	getAttributes(projectAttrs);
	int size = 0;
	for(int i=0; i<allAttrs.size(); i++){
		size += allAttrs[i].length;
	}
	buffer = malloc(size);
}

BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
                 TableScan *rightIn,           // TableScan Iterator of input S
                 const Condition &condition,   // Join condition
                 const unsigned numPages // # of pages that can be loaded into memory,
//   i.e., memory block size (decided by the optimizer)
) {
    this->left=leftIn;
    this->right=new TableScan(rightIn->rm,rightIn->tableName);
    this->condition=condition;
    
    bufferSize= numPages*PAGE_SIZE;
    blockData=malloc(bufferSize);
    
    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);
    
    for(lCondAttrIndex=0; lCondAttrIndex < this->leftAttrs.size(); lCondAttrIndex++) {
        if (strcmp(this->condition.lhsAttr.c_str(), this->leftAttrs[lCondAttrIndex].name.c_str()) == 0){
            break;
        }
    }
    if (this->condition.bRhsIsAttr) {
        for (rCondAttrIndex = 0; rCondAttrIndex < this->rightAttrs.size(); rCondAttrIndex++) {
            if (strcmp(this->condition.rhsAttr.c_str(), this->rightAttrs[rCondAttrIndex].name.c_str()) == 0)
                break;
        }
        attrType = leftAttrs[lCondAttrIndex].type;
        /*if (this->leftAttrs[lCondAttrIndex].type != this->rightAttrs[rCondAttrIndex].type) {
            return;
        } else {
            attrType = leftAttrs[lCondAttrIndex].type;
        }*/
    } else {
        rCondAttrIndex = -1;
        attrType = leftAttrs[lCondAttrIndex].type;
        /*if (this->leftAttrs[lCondAttrIndex].type!= this->condition.rhsValue.type) {
            return;
        } else {
            attrType = leftAttrs[lCondAttrIndex].type;
        }*/
    }
    
    this->maxRecrodLength = ceil(this->leftAttrs.size()/8.0);
    for(int i=0;i<this->leftAttrs.size();i++){
        switch(this->leftAttrs[i].type){
            case TypeInt:
                this->maxRecrodLength += sizeof(int);
                break;
            case TypeReal:
                this->maxRecrodLength += sizeof(int);
                break;
            case TypeVarChar:
                this->maxRecrodLength += this->leftAttrs[i].length;
                this->maxRecrodLength += sizeof(int);
                break;

        }
    }
    
    //currentBlockMapperIndex = 0;
    currentProperVectorIndex = 0;
    loadBlock();
}


BNLJoin::~BNLJoin() {
    free(blockData);
    blockMapper.clear();
}

RC BNLJoin::getNextTuple(void *data) {
    
    if(this->blockMapper.size() ==0)
        return -1;
    else{
        map<Key, vector<Pair> >::iterator it = blockMapper.begin();
        
        Key currentKey = it->first;
        Pair currentPair = (it->second)[currentProperVectorIndex];
        
        char* rightData = new char[PAGE_SIZE];
        
        while(right->getNextTuple(rightData)==0){
            char* rightAttrValue = new char[PAGE_SIZE];
            if(this->rCondAttrIndex != -1){
                JoinUtil::getValueAt(rightData,this->rightAttrs,this->rCondAttrIndex,rightAttrValue);
            }else{
                if(this->condition.rhsValue.type == TypeInt || this->condition.rhsValue.type==TypeReal){
                    memcpy(rightAttrValue,this->condition.rhsValue.data, sizeof(int));
                }
                else{
                    int length = *(int*)(this->condition.rhsValue.data);
                    memcpy(rightAttrValue,this->condition.rhsValue.data, sizeof(int)+length);
                }
            }
            if(this->attrType!=TypeVarChar){
                if(RBFM_ScanIterator::compareNum(currentKey.data,this->condition.op,rightAttrValue,this->attrType)){
                    char* leftData = new char[PAGE_SIZE];
                    memcpy(leftData,(char*)this->blockData+currentPair.pos,currentPair.length);
                    JoinUtil::combineData(leftData, this->leftAttrs, rightData, this->rightAttrs, (char*)data);
                    delete[] leftData;
                    delete[] rightAttrValue;
                    delete[] rightData;
                    return 0;
                }
            }
            else{
                int leftLen;
                memcpy(&leftLen,currentKey.data,sizeof(int));
                int rightLen;
                memcpy(&rightLen,rightAttrValue,sizeof(int));
                if(RBFM_ScanIterator::compareVarChar(leftLen, (char*)currentKey.data+sizeof(int), condition.op, rightLen, (char*)rightAttrValue+sizeof(int))){
                    char* leftData = new char[PAGE_SIZE];
                    memcpy(leftData,(char*)this->blockData+currentPair.pos,currentPair.length);
                    JoinUtil::combineData(leftData, this->leftAttrs, rightData, this->rightAttrs, (char*)data);
                    delete[] leftData;
                    delete[] rightAttrValue;
                    delete[] rightData;
                    return 0;
                }
            }
            delete[] rightAttrValue;
        }
        delete[] rightData;
        
        TableScan* right= new TableScan(this->right->rm, this->right->tableName);
        delete this->right;
        this->right = right;
        
        if(currentProperVectorIndex<it->second.size()-1){
            currentProperVectorIndex++;
            return getNextTuple(data);
        }else{
            this->blockMapper.erase(it->first);
            if(this->blockMapper.size() ==0){
                this->loadBlock();
                currentProperVectorIndex=0;
            }
            return getNextTuple(data);
        }
    }
    
}

RC BNLJoin::loadBlock() {
    if(blockData)
        free(blockData);
    if(!blockMapper.empty())
        blockMapper.clear();
    blockData = malloc(bufferSize);
    
    void* data = malloc(PAGE_SIZE);
    int offset = 0;
    
    while ((bufferSize-maxRecrodLength)>offset && left->getNextTuple(data)==0) {
        RecordBasedFileManager *rbf_manager=RecordBasedFileManager::instance();
        int recordLength = rbf_manager->getRecordSize(this->leftAttrs,data);
        
        memcpy((char*)blockData+offset, data, recordLength);
        void* page = malloc(PAGE_SIZE);
        JoinUtil::getValueAt(data, this->leftAttrs, this->lCondAttrIndex, page);
        Key* key = new Key(this->attrType, page);
        vector<Pair> container = blockMapper[*key];
        Pair pair(offset,recordLength);
        offset += recordLength;
        
        if (!container.empty()) {
            container.push_back(pair);
        } else {
            vector<Pair> pairs;
            pairs.push_back(pair);
            blockMapper[*key] = pairs;
        }
        
        delete key;
        free(page);
    }
    //currentBlockMapperIndex = 0;
    currentProperVectorIndex = 0;
    return 0;
}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const {
    for(int i=0; i< this->leftAttrs.size(); i++)
        attrs.push_back(this->leftAttrs[i]);
    
    for(int i=0; i< this->rightAttrs.size(); i++)
        attrs.push_back(this->rightAttrs[i]);
}
