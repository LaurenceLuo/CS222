
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
	this->input = input;
	this->condition = condition;
	getAttributes(attrs);
	value = malloc(sizeof(int));
}

// ... the rest of your implementations go here
