
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
    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);
    this->left=leftIn;
    this->right=new TableScan(rightIn->rm,rightIn->tableName);
    this->condition=condition;
    
    bufferSize= numPages*PAGE_SIZE;
    blockData=malloc(bufferSize);

    lCondAttrIndex=getAttrIndex(this->leftAttrs,this->condition.lhsAttr);
    if (this->condition.bRhsIsAttr) {
        rCondAttrIndex=getAttrIndex(this->rightAttrs,this->condition.rhsAttr);
        attrType = leftAttrs[lCondAttrIndex].type;
    } else {
        rCondAttrIndex = -1;
        attrType = leftAttrs[lCondAttrIndex].type;
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

    currentIndex = 0;
    loadBlock();
}


BNLJoin::~BNLJoin() {
    free(blockData);
    blockMap.clear();
}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const {
    for(int i=0; i< this->leftAttrs.size(); i++)
        attrs.push_back(this->leftAttrs[i]);

    for(int i=0; i< this->rightAttrs.size(); i++)
        attrs.push_back(this->rightAttrs[i]);
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition){
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);
	this->lVal = malloc(sizeof(int));
	this->rVal = malloc(sizeof(int));
	this->lData = malloc(PAGE_SIZE);
	this->rData = malloc(PAGE_SIZE);
}

Aggregate::Aggregate(Iterator *input,                              // Iterator of input R
                     Attribute aggAttr,                            // The attribute over which we are computing an aggregate
                     AggregateOp op                                // Aggregate operation
) {
    this->input = input;
    this->aggAttr = aggAttr;
    this->op = op;
    input->getAttributes(attrs);
    int aggAttrIndex = 0;
    for (int i=0; i< attrs.size(); i++){
        if (attrs[i].name.compare(aggAttr.name) == 0) break;
        aggAttrIndex += 1;
    }
    aggData.init(attrs[aggAttrIndex].type, op);
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute gAttr,AggregateOp op){}
