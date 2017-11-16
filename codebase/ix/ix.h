#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;

/*
typedef struct
{
    char type=1;//1 Leaf, 2 Nonleaf
    short size=0;
    int next=0;
    int prev=0;

} NodeDesc;

typedef struct
{
    int left;
    int right;
    int size;
    void* value;

} Key;

const int UpperThreshold=PAGE_SIZE-sizeof(NodeDesc);
const int LowerThreshold=(PAGE_SIZE-sizeof(NodeDesc))*0.5;
 *
 */

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

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        RecordBasedFileManager *rbf_manager;
};

typedef enum {Index = 0, Leaf} NodeType;

class BtreeNode{
	public:
		int nodeID;	// page ID
		NodeType nodeType;
		AttrType attrType;
		int d; // order: d <= m <= 2d

		// only for leaf
		int leftSibling;
		int rightSibling;

		vector<void *> keys;
		vector<int> childList;
		vector<vector<RID> > buckets;

		void initData(void *data);
		RC compareKey(const void *key, const void *value, AttrType attrType);
		int getKeyIndex(const void *key);
		int getChildIndex(const void *key, int keyIndex);
		RC insertIndex(const void *key, const int &childNodeID);
		RC insertLeaf(const void *key, const RID &rid);

		RC readEntry(IXFileHandle &ixfileHandle);
		RC writeEntry(IXFileHandle &ixfileHandle);

	protected:
		BtreeNode();
		~BtreeNode();

	private:
};

class Btree{
	public:
		int rootID;
		AttrType attrType;
		int d;

		RC createNode(IXFileHandle &ixfileHandle, BtreeNode &node, NodeType nodeType);
		RC readNode(IXFileHandle &ixfileHandle, int nodeID, BtreeNode &node);
		RC writeNode(IXFileHandle &ixfileHandle, BtreeNode &node);

		RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);
		RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);
		int findEntry(IXFileHandle &ixfileHandle, const void *key);

	private:

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
