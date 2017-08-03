#include "split.h"

std::vector<std::string> &split(const std::string &s, const char delim, std::vector<std::string> &elems, unsigned int maxElements) 
{
	std::stringstream ss(s);
	
	std::string item;
	
	while(((maxElements && (elems.size() == (maxElements - 1))) ? std::getline(ss, item) : std::getline(ss, item, delim)))
	{
		elems.push_back(item);
	}
	
	if((!maxElements || elems.size() < maxElements) && s.c_str()[s.length()-1] == delim)
		elems.push_back("");
	
	return elems;
}


std::vector<std::string> split(const std::string &s, const char delim, unsigned int maxElements) 
{
	std::vector<std::string> elems;
	split(s, delim, elems, maxElements);
	return elems;
}
