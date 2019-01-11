
/*! \example dictionary_compressed_column.hpp
 * This is an example of how to implement a compression technique in our framework. One has to inherit from an abstract base class CoGaDB::CompressedColumn and implement the pure virtual methods.
 */
#include <string>
#include <float.h>
#include <math.h> 
#include <bits/stdc++.h>
#include <map>
#include <iostream>
#include <iterator>
#include <vector>
#include <algorithm>
#include <stdlib.h> 
#include <cmath> 


#pragma once

#include <core/compressed_column.hpp>

//#include <boost/lexical_cast.hpp>

namespace CoGaDB{
	

/*!
 *  \brief     This class represents a dictionary compressed column with type T, is the base class for all compressed typed column classes.
 */	
template<class T>
class DeltaCompressedColumn : public CompressedColumn<T>{
	public:
	/***************** constructors and destructor *****************/
	DeltaCompressedColumn(const std::string& name, AttributeType db_type);
	virtual ~DeltaCompressedColumn();

	virtual bool insert(const boost::any& new_Value);
	virtual bool insert(const T& new_value);
	virtual T decompress(const int index);
	//virtual bool insert(const std::string& new_value);
	
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
	 
	 T t;
	 T sum;
	 T delt;
	 T c;
	 T org;

};

/***************** Start of Implementation Section ******************/

	
	template<class T>
	DeltaCompressedColumn<T>::DeltaCompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type), type_tid_comparator(), values_(), t(),sum(),delt(), c(), org(){

	}

	template<class T>
	DeltaCompressedColumn<T>::~DeltaCompressedColumn(){

	}

	template<class T>
	std::vector<T>& DeltaCompressedColumn<T>::getContent(){
		return values_;
	}

	template<class T>
	bool DeltaCompressedColumn<T>::insert(const boost::any& new_value){
		if(new_value.empty()) return false;
		if(typeid(T)==new_value.type()){
			 T value = boost::any_cast<T>(new_value);
			 values_.push_back(value);
			 return true;
		}
		return false;
	}

	template<class T>
	bool DeltaCompressedColumn<T>::insert(const T& new_value){
      	
      	T original = new_value;
      	T change = new_value;

      	change -= t;
      	t= original;

        values_.push_back(change);
        
		return true;
	}


 //    template<>
	// bool DeltaCompressedColumn<std::string>::insert(const std::string& new_value){

            		
	// return true;
	// }

	template<class T>
	T& DeltaCompressedColumn<T>::operator[](const int index){


		  delt = decompress(index);
		 return delt;
	}


	template<class T>
	T DeltaCompressedColumn<T>::decompress(const int index){

	  sum = 0;
		for(int i=0;i<=index;i++){
			sum+= values_[i] ;
		}

	 	return sum;
	}


	template<class T>
	bool DeltaCompressedColumn<T>::update(TID tid, const boost::any& new_value ){
	    typename std::vector<T>::size_type sz = values_.size();	
 		
 		sum = 0;

		for (unsigned i=0;i<sz;i++) {
			values_[i] += sum;
			sum = values_[i];
		 }
		
		T value = boost::any_cast<T>(new_value);
		  values_[tid] = value;

		c = 0;
		for (unsigned i=0;i<sz;i++) {
			org = values_[i];
			values_[i] -= c;
			c = org;
		}		
		return true;
	}

	template <typename T> 
	template <typename InputIterator>
	bool DeltaCompressedColumn<T>::insert(InputIterator first , InputIterator last){
		this->values_.insert(this->values_.end(),first,last);
		return true;
	}

	template<class T>
	const boost::any DeltaCompressedColumn<T>::get(TID tid){
				if(tid<values_.size())
 			return boost::any(values_[tid]);
		else{
			std::cout << "fatal Error!!! Invalid TID!!! Attribute: " << this->name_ << " TID: " << tid  << std::endl;
		}
		return boost::any();

	}

	template<class T>
	void DeltaCompressedColumn<T>::print() const throw(){
		std::cout << "| " << this->name_ << " |" << std::endl;
		std::cout << "________________________" << std::endl;
		for(unsigned int i=0;i<values_.size();i++){
			std::cout << "| " << values_[i] << " |" << std::endl;
		}
	}
	template<class T>
	size_t DeltaCompressedColumn<T>::size() const throw(){
		return values_.size();
	}

	template<class T>
	const ColumnPtr DeltaCompressedColumn<T>::copy() const{
		return ColumnPtr(new DeltaCompressedColumn<T>(*this));
	}



	template<class T>
	bool DeltaCompressedColumn<T>::update(PositionListPtr tids, const boost::any& new_value){
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
	bool DeltaCompressedColumn<T>::remove(TID tid){
	
	typename std::vector<T>::size_type sz = values_.size();
		sum = 0;

		for (unsigned i=0;i<sz;i++) {
			values_[i] += sum;
			sum = values_[i];
		}
		
        values_.erase(values_.begin()+tid);

		c = 0;
		for (unsigned i=0;i<sz;i++) {
			org = values_[i];
			values_[i] -= c;
			c = org;
		}

		return true;	
	}
	
	
	template<class T>
	bool DeltaCompressedColumn<T>::remove(PositionListPtr tids){
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
	bool DeltaCompressedColumn<T>::clearContent(){
		values_.clear();
		return true;
	}

	template<class T>
	bool DeltaCompressedColumn<T>::store(const std::string& path_){
		std::string path(path_);
		path += "/";
		path += this->name_;
		//std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
		std::ofstream outfile (path.c_str(),std::ios_base::binary | std::ios_base::out);
		boost::archive::binary_oarchive oa(outfile);

		oa << values_;

		outfile.flush();
		outfile.close();
		return true;
	}

	template<class T>
	bool DeltaCompressedColumn<T>::load(const std::string& path_){
		std::string path(path_);
		path += "/";
		path += this->name_;
		
		//std::cout << "Opening File '" << path << "'..." << std::endl;
		std::ifstream infile (path.c_str(),std::ios_base::binary | std::ios_base::in);
		boost::archive::binary_iarchive ia(infile);
		ia >> values_;
		infile.close();
		return true;
	}



	template<class T>
	unsigned int DeltaCompressedColumn<T>::getSizeinBytes() const throw(){
		return values_.capacity()*sizeof(T);
	}

	template<>
	inline unsigned int DeltaCompressedColumn<std::string>::getSizeinBytes() const throw(){
		unsigned int size_in_bytes=0;
		for(unsigned int i=0;i<values_.size();++i){
			size_in_bytes+=values_[i].capacity();
		}
		return size_in_bytes;
	}

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

