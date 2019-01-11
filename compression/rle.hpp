
/*! \example dictionary_compressed_column.hpp
 * This is an example of how to implement a compression technique in our framework. One has to inherit from an abstract base class CoGaDB::CompressedColumn and implement the pure virtual methods.
 */
#include <string>
#include <map>
#include <iostream>
#include <iterator>
#include <vector>
#include <algorithm>
#pragma once

#include <core/compressed_column.hpp>

namespace CoGaDB{
	

/*!
 *  \brief     This class represents a dictionary compressed column with type T, is the base class for all compressed typed column classes.
 */	
template<class T>
class rleCompressedColumn : public CompressedColumn<T>{
	public:
	/***************** constructors and destructor *****************/
	rleCompressedColumn(const std::string& name, AttributeType db_type);
	virtual ~rleCompressedColumn();

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


	
	virtual T& operator[](const int indxex);
	std::vector<T>& getContent();

	private:

		struct Type_TID_Comparator {
  			inline bool operator() (std::pair<T,TID> i, std::pair<T,TID> j) { return (i.first<j.first);}
		} type_tid_comparator;


	std::vector<T> values_;
    std::vector<int> run_;
    int i;

};

/***************** Start of Implementation Section ******************/

	
	template<class T>
	rleCompressedColumn<T>::rleCompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type), type_tid_comparator(), values_(), run_(), i(){

	}

	template<class T>
	rleCompressedColumn<T>::~rleCompressedColumn(){

	}

	template<class T>
	std::vector<T>& rleCompressedColumn<T>::getContent(){
		return values_;
	}

	template<class T>
	bool rleCompressedColumn<T>::insert(const boost::any& new_value){
		if(new_value.empty()) return false;
		if(typeid(T)==new_value.type()){
			 T value = boost::any_cast<T>(new_value);
			 values_.push_back(value);
			 return true;
		}
		return false;
	}


	template<class T>
	bool rleCompressedColumn<T>::insert(const T& new_value){
	 	typename std::vector<T>::size_type sz = values_.size();

		if(sz == 0){
			values_.push_back(new_value);
			run_.push_back(1);
		}
		else{
		if(values_[sz-1] != new_value) {
			values_.insert(values_.begin()+sz,new_value);
			run_.insert(run_.begin()+sz,1);
			//std::cout << values_[sz]; 
		}
		else{ 
			int n = sz-1;
			run_.insert(run_.begin()+n,1);}
		}
	    return true;
	}

	template<class T>
	T& rleCompressedColumn<T>::operator[](const int indxex){
		short unsigned int indx=0,i;
		for(i=0;indx<indxex;i++){
			indx +=run_.at(i);
		}
		return values_.at(i);
	}

	template <typename T> 
	template <typename InputIterator>
	bool rleCompressedColumn<T>::insert(InputIterator first , InputIterator last){
		this->values_.insert(this->values_.end(),first,last);
		return true;
	}

	template<class T>
	const boost::any rleCompressedColumn<T>::get(TID tid){
				if(tid<values_.size())
 			return boost::any(values_[tid]);
		else{
			std::cout << "fatal Error!!! Invalid TID!!! Attribute: " << this->name_ << " TID: " << tid  << std::endl;
		}
		return boost::any();

	}

	template<class T>
	void rleCompressedColumn<T>::print() const throw(){
		std::cout << "| " << this->name_ << " |" << std::endl;
		std::cout << "________________________" << std::endl;
		for(unsigned int i=0;i<values_.size();i++){
			std::cout << "| " << values_[i] << " |" << std::endl;
		}
	}
	template<class T>
	size_t rleCompressedColumn<T>::size() const throw(){
		return values_.size();
	}
	template<class T>
	const ColumnPtr rleCompressedColumn<T>::copy() const{
		return ColumnPtr(new rleCompressedColumn<T>(*this));
	}

	template<class T>
	bool rleCompressedColumn<T>::update(TID tid, const boost::any& new_value ){
		unsigned int i,indx=0;
		T value = boost::any_cast<T>(new_value);
		for(i=0;indx<tid;i++){
			indx +=run_.at(i);
		}
		if(run_.at(i)==1)
			values_.at(i) = value;
		else{
			
			if(indx==tid)
			{
				run_.at(i)--;
				values_.insert(values_.begin()+i,value);
				run_.insert(run_.begin()+i,1);	
			}			
			else{
				int diff = indx-tid;
				run_.at(i) = diff-1;
				values_.insert(values_.begin()+i+1,value);
				run_.insert(run_.begin()+i+1,1);
				values_.insert(values_.begin()+i+2,values_.at(i));
				run_.insert(run_.begin()+i+2,diff);
						
			}
		}
		return true;
	}

	template<class T>
	bool rleCompressedColumn<T>::update(PositionListPtr tids, const boost::any& new_value){
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
	bool rleCompressedColumn<T>::remove(TID tid){
		unsigned int i,indx=0;
		for(i=0;indx<tid;i++){
			indx +=run_.at(i);
		}
		if(run_.at(i)==1){
			run_.erase(run_.begin()+i);
			values_.erase(values_.begin()+i);	
		}
		else{
			run_.at(i)--;
		}
	return true;	
	}
	
	template<class T>
	bool rleCompressedColumn<T>::remove(PositionListPtr tids){
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
	bool rleCompressedColumn<T>::clearContent(){
		values_.clear();
		return true;
	}

	template<class T>
	bool rleCompressedColumn<T>::store(const std::string& path_){
		std::string path(path_);
		path += "/";
		std::string newpath(path);
		path += this->name_;
		newpath += "newpath";
		//std::cout << "Writing Column " << this->getName() << " to File " << path << std::endl;
		std::ofstream outvfile (path.c_str(),std::ios_base::binary | std::ios_base::out);
		boost::archive::binary_oarchive opv(outvfile);
		opv << values_;

		std::ofstream outrfile (newpath.c_str(),std::ios_base::binary | std::ios_base::out);
		boost::archive::binary_oarchive opr(outrfile);
		opr <<run_;

		outvfile.flush();
		outvfile.close();
		outrfile.flush();
		outrfile.close();
		return true;
	}
  
    template<class T>
	bool rleCompressedColumn<T>::load(const std::string& path_){
		try{std::string path(path_);
		//std::cout << "Loading column '" << this->name_ << "' from path '" << path << "'..." << std::endl;
		//string path("data/");
		path += "/";
		std::string newpath(path);
		path += this->name_;
		newpath += "newpath";
		
		//std::cout << "Opening File '" << path << "'..." << std::endl;
		std::ifstream invfile (path.c_str(),std::ios_base::binary | std::ios_base::in);
		boost::archive::binary_iarchive ipv(invfile);
		ipv >> values_;

		std::ifstream inrfile (newpath.c_str(),std::ios_base::binary | std::ios_base::in);
		boost::archive::binary_iarchive ipr(inrfile);
		ipr >> run_;
		invfile.close();
		inrfile.close();}catch(std::exception e){std::cout<<"error in store";}
		return true;
	}

	template<class T>
	unsigned int rleCompressedColumn<T>::getSizeinBytes() const throw(){
		return values_.capacity()*sizeof(T);
	}

		template<>
	inline unsigned int rleCompressedColumn<std::string>::getSizeinBytes() const throw(){
		unsigned int size_in_bytes=0;
		for(unsigned int i=0;i<values_.size();++i){
			size_in_bytes+=values_[i].capacity();
		}
		return size_in_bytes;
	}

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

