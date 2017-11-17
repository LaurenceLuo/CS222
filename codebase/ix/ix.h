#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;

typedef enum {Index = 0, Leaf} NodeType;
typedef vector<RID> RidList;

class BtreeNode{
	public:
		char nodePage[PAGE_SIZE];
		int nodeID;	// page ID
		NodeType nodeType;
		AttrType attrType;
		int attrLen;
		int deleteMark;
		int d; // order: d <= m <= 2d

		// only for leaf
		int leftSibling;
		int rightSibling;

		vector<void *> keys;
		vector<int> childList;
		vector<RidList> buckets;

		void getData(void *data);
		void setData(BtreeNode *node);
		RC compareKey(const void *key, const void *value, AttrType attrType);
		int getKeyIndex(const void *key);
		int getChildIndex(const void *key, int keyIndex);
		RC insertIndex(const void *key, const int &childNodeID);
		RC insertLeaf(const void *key, const RID &rid);

		// Leaf node
		RC readEntry(IXFileHandle &ixfileHandle);
		RC writeEntry(IXFileHandle &ixfileHandle);

        BtreeNode();
        ~BtreeNode(){};

};

//	Store meta data of Btree to page 0
class Btree{
	public:
		int rootID;
		AttrType attrType;
		int d;
		int attrLen;

		RC createNode(IXFileHandle &ixfileHandle, BtreeNode &node, NodeType nodeType);
		RC readNode(IXFileHandle &ixfileHandle, int nodeID, BtreeNode &node);
		RC writeNode(IXFileHandle &ixfileHandle, BtreeNode &node);

		RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);
		RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);
		int findEntryPID(IXFileHandle &ixfileHandle, const void *key);

        Btree();
        ~Btree(){};

	private:
		RC recursiveInsert(IXFileHandle &ixfileHandle, const void *key, const RID &rid, int nodeID);
		int recursiveFind(IXFileHandle &ixfileHandle, const void *key, int nodeID);
};

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        RC readBtree(IXFileHandle &ixfileHandle, Btree *btree);
        RC writeBtree(IXFileHandle &ixfileHandle, const Btree *btree);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
        void recursivePrint(IXFileHandle &ixfileHandle, const Attribute &attribute, Btree *btree, BtreeNode *node, int depth);

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        PagedFileManager *_pfm_manager;
};


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};



class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;
    FileHandle fileHandle;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

#endif
