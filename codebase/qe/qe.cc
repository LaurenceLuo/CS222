
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
