/*! \example dictionary_compressed_column.hpp
 * This is an example of how to implement a compression technique in our framework. One has to inherit from an abstract base class CoGaDB::CompressedColumn and implement the pure virtual methods.
 */
#include <string>
#include <map>
#include <iostream>
#include <fstream>
#include <iterator>
#include <vector>
#include <algorithm>

#pragma once

#include <core/compressed_column.hpp>
#include <boost/type_traits/remove_const.hpp>
#include <boost/serialization/map.hpp>

namespace CoGaDB{
	

/*!
 *  \brief     This class represents a dictionary compressed column with type T, is the base class for all compressed typed column classes.
 */	
template<class T>
class DictionaryCompressedColumn : public CompressedColumn<T>{
	public:
	/***************** constructors and destructor *****************/
	DictionaryCompressedColumn(const std::string& name, AttributeType db_type);
	virtual ~DictionaryCompressedColumn();

	virtual bool insert(const boost::any& new_Value);
	virtual bool insert(const T& new_value);
	template <typename InputIterator>
	bool insert(InputIterator first, InputIterator last);

	virtual bool update(TID tid, const boost::any& new_value);
	virtual bool update(PositionListPtr tid, const boost::any& new_value);	
	
	virtual bool remove(TID tid);
	//assumes tid list is sorted ascending
	virtual bool remove(PositionListPtr tid);
	virtual bool clearContent();

	virtual const boost::any get(TID tid);
	//virtual const boost::any* const getRawData()=0;
	virtual void print() const throw();
	virtual size_t size() const throw();
	virtual unsigned int getSizeinBytes() const throw();

	virtual const ColumnPtr copy() const;

	virtual bool store(const std::string& path);
	virtual bool load(const std::string& path);


	
	virtual T& operator[](const int index);
	std::vector<T>& getContent();

	private:

		struct Type_TID_Comparator {
  			inline bool operator() (std::pair<T,TID> i, std::pair<T,TID> j) { return (i.first<j.first);}
		} type_tid_comparator;


	std::vector<T> values_;
	std::map<T,T> dictionary;
	 
	T it;
	int cnt;
	T key;

};

/***************** Start of Implementation Section ******************/

	
	template<class T>
	DictionaryCompressedColumn<T>::DictionaryCompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type), type_tid_comparator(), values_(), dictionary(), it(), cnt(), key(){

	}

	template<class T>
	DictionaryCompressedColumn<T>::~DictionaryCompressedColumn(){

	}

	template<class T>
	std::vector<T>& DictionaryCompressedColumn<T>::getContent(){
		return values_;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::insert(const boost::any& new_value){
		if(new_value.empty()) return false;
		if(typeid(T)==new_value.type()){
			 T value = boost::any_cast<T>(new_value);
			 values_.push_back(value);
			 return true;
		}
		return false;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::insert(const T& new_value){

		typename std::map<T,T>::iterator it1;
		 it1 = dictionary.find(new_value);
		  
            if (it1 != dictionary.end())
            {
            	//found
       			values_.push_back(dictionary[new_value]); 
            }
            else
            {
				dictionary.insert(std::pair<T,T>(new_value,it));
                values_.push_back(it);
		        cnt++;
                it = boost::lexical_cast<T> (cnt);
            }
	    return true;
	}

		template<class T>
	T& DictionaryCompressedColumn<T>::operator[](const int index){
		T value = values_[index];
		
		typename std::map<T,T>::iterator it;
		for (it = dictionary.begin(); it != dictionary.end(); ++it)
        {
            if (it->second == value)
            {
               key = it->first;
              //std::cout <<"compiler love you if you print this"<< key << std::endl;  
            }
        }
		return key;
	}


	template <typename T> 
	template <typename InputIterator>
	bool DictionaryCompressedColumn<T>::insert(InputIterator first , InputIterator last){
		this->values_.insert(this->values_.end(),first,last);
		return true;
	}

	template<class T>
	const boost::any DictionaryCompressedColumn<T>::get(TID tid){
				if(tid<values_.size())
 			return boost::any(values_[tid]);
		else{
			std::cout << "fatal Error!!! Invalid TID!!! Attribute: " << this->name_ << " TID: " << tid  << std::endl;
		}
		return boost::any();

	}

	template<class T>
	void DictionaryCompressedColumn<T>::print() const throw(){
		std::cout << "| " << this->name_ << " |" << std::endl;
		std::cout << "________________________" << std::endl;
		for(unsigned int i=0;i<values_.size();i++){
			std::cout << "| " << values_[i] << " |" << std::endl;
		}
	}
	template<class T>
	size_t DictionaryCompressedColumn<T>::size() const throw(){
		return values_.size();
	}
	template<class T>
	const ColumnPtr DictionaryCompressedColumn<T>::copy() const{
		return ColumnPtr(new DictionaryCompressedColumn<T>(*this));
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::update(TID tid, const boost::any& new_value ){
		
		if(new_value.empty()) return false;
		
		T v =boost::any_cast<T>(new_value);

	    typename std::map<T,T>::iterator it;
		for (it = dictionary.begin(); it != dictionary.end(); ++it)
        {
        	if(it->first == v) {
        		values_[tid] = it->second;
        		break;
        	}
             else
            {
               	T t = boost::lexical_cast<T> (++cnt);
               	values_[tid] = t;
               	dictionary.insert(std::pair<T,T>(v,t));
  			}
        }
    	return true;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::update(PositionListPtr tids, const boost::any& new_value){ 	
		if(!tids)
			return false;
	    if(new_value.empty()) return false;
		if(typeid(T)==new_value.type()){
			 T value = boost::any_cast<T>(new_value);
			 for(unsigned int i=0;i<tids->size();i++){
				TID tid=(*tids)[i];
				values_[tid]=value;
		    }
			return true;
		}else{
			std::cout << "Fatal Error!!! Typemismatch for column " << this->name_ << std::endl; 
		}
		return false;	
	}
	
	template<class T>
	bool DictionaryCompressedColumn<T>::remove(TID tid){
		values_.erase(values_.begin()+tid);
		return true;	
	}
	
	template<class T>
	bool DictionaryCompressedColumn<T>::remove(PositionListPtr tids){
		if(!tids)
			return false;
		//test whether tid list has at least one element, if not, return with error
		if(tids->empty())
			return false;	

		typename PositionList::reverse_iterator rit;

		for (rit = tids->rbegin(); rit!=tids->rend(); ++rit)
			values_.erase(values_.begin()+(*rit));
		return true;			
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::clearContent(){
		values_.clear();
		return true;
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::store(const std::string& path_){
			std::string path(path_);
		path += "/";
		std::string dictpath(path);
		path += this->name_;
		dictpath += "dictpath";
		//std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
		std::ofstream outfile (path.c_str(),std::ios_base::binary | std::ios_base::out);
		boost::archive::binary_oarchive oa(outfile);

		oa << values_;
		
		std::ofstream outlfile (dictpath.c_str(),std::ios_base::binary | std::ios_base::out);
		boost::archive::binary_oarchive olu(outlfile);
		olu <<dictionary;

		outfile.flush();
		outfile.close();
		outlfile.flush();
		outlfile.close();
		return true;
	}
	template<class T>
	bool DictionaryCompressedColumn<T>::load(const std::string& path_){
		try{std::string path(path_);
		path += "/";
		std::string dictpath(path);
		path += this->name_;
		dictpath += "dictpath";
		
		//std::cout << "Opening File '" << path << "'..." << std::endl;
		std::ifstream infile (path.c_str(),std::ios_base::binary | std::ios_base::in);
		boost::archive::binary_iarchive ia(infile);
		ia >> values_;
		
		std::ifstream infilen (dictpath.c_str(),std::ios_base::binary | std::ios_base::in);
		boost::archive::binary_iarchive ian(infilen);
		ian >> dictionary;
		
		infilen.close();
		infile.close();}catch(std::exception e){std::cout<<"error in store";}
		return true;
	}



	template<class T>
	unsigned int DictionaryCompressedColumn<T>::getSizeinBytes() const throw(){
		return values_.capacity()*sizeof(T);
	}

	template<>
	inline unsigned int DictionaryCompressedColumn<std::string>::getSizeinBytes() const throw(){
		unsigned int size_in_bytes=0;
		for(unsigned int i=0;i<values_.size();++i){
			size_in_bytes+=values_[i].capacity();
		}
		return size_in_bytes;
	}

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB
