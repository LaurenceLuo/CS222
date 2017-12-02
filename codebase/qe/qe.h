#ifndef _qe_h_
#define _qe_h_

#include <vector>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN=0, MAX, COUNT, SUM, AVG } AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }

            // Call RM scan to get an iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
            return iter->getNextTuple(rid, data);
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~TableScan()
        {
        	iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;


            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
        };
};


class Filter : public Iterator {
    // Filter operator
    public:
		Iterator *input;
		Condition condition;
		vector<Attribute> attrs;
		void *value;

        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );
        ~Filter(){
        		free(value);
        };

        RC getNextTuple(void *data) {
        		while(this->input->getNextTuple(data) != QE_EOF){
        			// right-hand side is an attribute
        			// need to get left value and right value and then compare
        			if(condition.bRhsIsAttr){
        				void *rightVal;
        				int offset = 0;
        				int leftLen = 0;
        				int rightLen = 0;
        				AttrType attrType;
        				for(int i=0; i<attrs.size(); i++){
        					// get left value
        					if(attrs[i].name.compare(condition.lhsAttr) == 0){
							switch(attrs[i].type){
								case TypeInt: {
									value = malloc(sizeof(int));
									memset(value, 0, sizeof(int));
									memcpy((char *)value, (char *)data + offset, sizeof(int));
									offset += sizeof(int);
									break;
								}
								case TypeReal: {
									value = malloc(sizeof(float));
									memset(value, 0, sizeof(float));
									memcpy((char *)value, (char *)data + offset, sizeof(float));
									offset += sizeof(float);
									break;
								}
								case TypeVarChar: {
									memcpy(&leftLen, (char *)data + offset, sizeof(int));
									offset += sizeof(int);
									value = malloc(leftLen+1);
									memset(value, 0, leftLen+1);
									memcpy((char *)value, (char *)data + offset, leftLen);
									offset += leftLen;
									break;
								}
							}
							attrType = attrs[i].type;
							continue;
        					}
        					// get right value
        					if(attrs[i].name.compare(condition.rhsAttr) == 0){
							switch(attrs[i].type){
								case TypeInt: {
									rightVal = malloc(sizeof(int));
									memset(rightVal, 0, sizeof(int));
									memcpy((char *)rightVal, (char *)data + offset, sizeof(int));
									offset += sizeof(int);
									break;
								}
								case TypeReal: {
									rightVal = malloc(sizeof(float));
									memset(rightVal, 0, sizeof(float));
									memcpy((char *)rightVal, (char *)data + offset, sizeof(float));
									offset += sizeof(float);
									break;
								}
								case TypeVarChar: {
									memcpy(&rightLen, (char *)data + offset, sizeof(int));
									offset += sizeof(int);
									rightVal = malloc(rightLen+1);
									memset(rightVal, 0, rightLen+1);
									memcpy((char *)rightVal, (char *)data + offset, rightLen);
									offset += rightLen;
									break;
								}
							}
							continue;
        					}
        					switch(attrs[i].type){
        						case TypeInt: {
        							offset += sizeof(int);
        							break;
        						}
        						case TypeReal: {
        							offset += sizeof(float);
        							break;
        						}
        						case TypeVarChar: {
        							int varCharLen = 0;
        							memcpy(&varCharLen, (char *)data + offset, sizeof(int));
        							offset += varCharLen + sizeof(int);
        						}
        					}
        				}
        				// compare two values
    	                if(attrType!=TypeVarChar){
    	                	//compareNum(void* storedValue, const CompOp compOp, const void *valueToCompare, AttrType type);
    	                		if(RBFM_ScanIterator::compareNum(value, condition.op, rightVal, attrType)){
    	                			free(rightVal);
    	                			return 0;
    	                		}
    	                }
    	                else{
    	                	//compareVarChar(int &storedValue_len, void* storedValue, const CompOp compOp, int &valueToCompare_len, const void *valueToCompare);
    	                		if(RBFM_ScanIterator::compareVarChar(leftLen, value, condition.op, rightLen, rightVal)){
    	                			free(rightVal);
    	                			return 0;
    	                		}
    	                }
        			}
        			else{
        				// right-hand side is value
        				// read attribute from data and compare value with condition
        				int offset = 0;
        				int leftLen = 0;
        				int rightLen = 0;
        				for(int i=0; i<attrs.size(); i++){
        					if(attrs[i].name.compare(condition.lhsAttr)==0){
        						switch(attrs[i].type){
        							case TypeInt: {
        								value = malloc(sizeof(int));
        								memset(value, 0, sizeof(int));
        								memcpy(value, (char *)data+offset, sizeof(int));
        								offset += sizeof(int);
        								break;
        							}
        							case TypeReal: {
        								value = malloc(sizeof(float));
        								memset(value, 0, sizeof(float));
        								memcpy(value, (char *)data+offset, sizeof(float));
        								offset += sizeof(float);
        								break;
        							}
        							case TypeVarChar: {
        								memcpy(&leftLen, (char *)data+offset, sizeof(int));
        								value = malloc(leftLen+1);
        								memset(value, 0, leftLen+1);
        								memcpy(value, (char *)data+offset, leftLen);
        								offset += leftLen + sizeof(int);
        								break;
        							}
        						}
        						if(attrs[i].type!=TypeVarChar){
        							if(RBFM_ScanIterator::compareNum(value, condition.op, condition.rhsValue.data, attrs[i].type)){
        								return 0;
        							}
        						}
        						else{
        							memcpy(&rightLen, (char *)condition.rhsValue.data, sizeof(int));
        							if(RBFM_ScanIterator::compareVarChar(leftLen, value, condition.op, rightLen, condition.rhsValue.data)){
        								return 0;
        							}
        						}
        					}
        					if(attrs[i].type != TypeVarChar){
        						offset += sizeof(int);
        					}
        					else{
        						int varCharLen = 0;
        						memcpy(&varCharLen, (char *)data+offset, sizeof(int));
        						offset += varCharLen + sizeof(int);
        					}
        				}
        			}
        		}
        		return QE_EOF;
        };
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{
        		this->input->getAttributes(attrs);
        };
};


class Project : public Iterator {
    // Projection operator
    public:
        Project(Iterator *input,                    // Iterator of input R
              const vector<string> &attrNames){};   // vector containing attribute names
        ~Project(){};

        RC getNextTuple(void *data) {return QE_EOF;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{};
};

class BNLJoin : public Iterator {
    // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
			                                 //   i.e., memory block size (decided by the optimizer)
        ){};
        ~BNLJoin(){};

        RC getNextTuple(void *data){return QE_EOF;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{};
};


class INLJoin : public Iterator {
    // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        ){};
        ~INLJoin(){};

        RC getNextTuple(void *data){return QE_EOF;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{};
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
    public:
      GHJoin(Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
      ){};
      ~GHJoin(){};

      RC getNextTuple(void *data){return QE_EOF;};
      // For attribute in vector<Attribute>, name it as rel.attr
      void getAttributes(vector<Attribute> &attrs) const{};
};

class Aggregate : public Iterator {
    // Aggregation operator
    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        ){};

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        ){};
        ~Aggregate(){};

        RC getNextTuple(void *data){return QE_EOF;};
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const{};
};

#endif
