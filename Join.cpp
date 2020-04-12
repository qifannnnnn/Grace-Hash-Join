#include "Join.hpp"
#include <functional>
/*
 * TODO: Student implementation
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */

vector<Bucket> partition(
    Disk* disk, 
    Mem* mem, 
    pair<unsigned int, unsigned int> left_rel, 
    pair<unsigned int, unsigned int> right_rel) {
    
    vector<Bucket> result;
    for(int i=0;i<MEM_SIZE_IN_PAGE-1;i++){
        result.push_back(Bucket(disk));
    }
    /*which one is smaller*/
  /*  int left_size=0;
    int right_size=0;
    pair<unsigned int, unsigned int> temp;
    for (unsigned int i=left_rel.first;i<left_rel.second;i++){
        left_size+=disk->diskRead(i)->size();
    }
    for (unsigned int i=right_rel.first;i<right_rel.second;i++){
           right_size+=disk->diskRead(i)->size();
       }
    
    if (right_size<left_size){
        temp=left_rel;
        left_rel=right_rel;
        right_rel=temp;
    }*/
    /*partition the left_rel*/
    for (unsigned int i=left_rel.first;i<left_rel.second;i++){
        /*for each page in the disk, load it to memory first*/
        mem->loadFromDisk(disk, i, 0);
        for (int j=0;j<mem->mem_page(0)->size();j++){
            /*get the record and hash it*/
            Record r=mem->mem_page(0)->get_record(j);
            
            int num=r.partition_hash()%(MEM_SIZE_IN_PAGE-1)+1;
            /*check if the buffer corresponding to the hash value is full*/
            if (mem->mem_page(num)->full()){
                /*load it to the disk*/
                result[num-1].add_left_rel_page(mem->flushToDisk(disk,num));
                mem->mem_page(num)->reset();
            }
            mem->mem_page(num)->loadRecord(r);
        }
    }
    
    for (int i=1;i<MEM_SIZE_IN_PAGE;i++){
        if (mem->mem_page(i)->size()>0){
            result[i-1].add_left_rel_page(mem->flushToDisk(disk, i));}
        mem->mem_page(i)->reset();
        
    }
    
    mem->mem_page(0)->reset();
    
    for (int i=right_rel.first;i<right_rel.second;i++){
           /*for each page in the disk, load it to memory first*/
           mem->loadFromDisk(disk, i, 0);
           for (int j=0;j<mem->mem_page(0)->size();j++){
               /*get the record and hash it*/
               Record r=mem->mem_page(0)->get_record(j);
               int num=r.partition_hash()%(MEM_SIZE_IN_PAGE-1)+1;
               
               /*check if the buffer corresponding to the hash value is full*/
               if (mem->mem_page(num)->full()){
                   /*load it to the disk*/
                   result[num-1].add_right_rel_page(mem->flushToDisk(disk,num));
                   mem->mem_page(num)->reset();
               }
               mem->mem_page(num)->loadRecord(r);
           }
       }
    
    for (int i=1;i<MEM_SIZE_IN_PAGE;i++){
        if(mem->mem_page(i)->size()>0){
            result[i-1].add_right_rel_page(mem->flushToDisk(disk, i));}
           mem->mem_page(i)->reset();
           
       }
    
     mem->mem_page(0)->reset();
    
    return result;
    
}

/*
 * TODO: Student implementation
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */

vector<unsigned int> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
    vector<unsigned int> result;
    /*bring out the left part in the main memory*/
    
    /*determine which one is smaller */
    int left_size=0;
    int right_size=0;
    for (int i=0;i<partitions.size();i++){
        for (int j:partitions[i].get_left_rel()){
            left_size+=disk->diskRead(j)->size();
        }
        for (int j:partitions[i].get_right_rel()){
            right_size+=disk->diskRead(j)->size();
               }
        
    }
    
    
    if (left_size<right_size){
    
    for (int i=0;i<partitions.size();i++){
        
        /*j is the number of disk*/
        for (int j:partitions[i].get_left_rel()){
            if (partitions[i].get_right_rel().size()==0){
                break;
            }
            mem->loadFromDisk(disk,j , 0);
            /*for each record hash it and put it in the right place*/
            for (int k=0; k<mem->mem_page(0)->size();k++){
                Record r=mem->mem_page(0)->get_record(k);
                 
                int hash=r.probe_hash()%(MEM_SIZE_IN_PAGE-2)+1;
                mem->mem_page(hash)->loadRecord(r);
            }
        }
        
        /*bring up one from right each time*/
        for (int j:partitions[i].get_right_rel()){
            mem->loadFromDisk(disk, j, 0);
            /*do the comparison*/
            for (int k=0; k<mem->mem_page(0)->size();k++){
                Record r=mem->mem_page(0)->get_record(k);
                int hash=r.probe_hash()%(MEM_SIZE_IN_PAGE-2)+1;
                /*==*/
                for (int t=0;t<mem->mem_page(hash)->size();t++){
                    if (r==mem->mem_page(hash)->get_record(t)){
                        /*put it in the output page*/
                        /*if the output page is full*/
                        if (mem->mem_page(MEM_SIZE_IN_PAGE-1)->full()){
                            result.push_back(mem->flushToDisk(disk, MEM_SIZE_IN_PAGE-1));
                            mem->mem_page(MEM_SIZE_IN_PAGE-1)->reset();
                        }
                        /*...*/
                        mem->mem_page(MEM_SIZE_IN_PAGE-1)->loadPair(mem->mem_page(hash)->get_record(t), r);
                    }
                }
            }
        }
        
        for (int n=1;n<=MEM_SIZE_IN_PAGE-2;n++){
            mem->mem_page(n)->reset();
        }
    
    }
    /*in the end*/
        if (mem->mem_page(MEM_SIZE_IN_PAGE-1)->size()>0){
            result.push_back(mem->flushToDisk(disk, MEM_SIZE_IN_PAGE-1));}}
    
    else{
        for (int i=0;i<partitions.size();i++){
               
               /*j is the number of disk*/
               for (int j:partitions[i].get_right_rel()){
                   if (partitions[i].get_left_rel().size()==0){
                                  break;
                              }
                   mem->loadFromDisk(disk,j , 0);
                   /*for each record hash it and put it in the right place*/
                   for (int k=0; k<mem->mem_page(0)->size();k++){
                       Record r=mem->mem_page(0)->get_record(k);
                        
                       int hash=r.probe_hash()%(MEM_SIZE_IN_PAGE-2)+1;
                       mem->mem_page(hash)->loadRecord(r);
                   }
               }
               
               /*bring up one from right each time*/
               for (int j:partitions[i].get_left_rel()){
                   mem->loadFromDisk(disk, j, 0);
                   /*do the comparison*/
                   for (int k=0; k<mem->mem_page(0)->size();k++){
                       Record r=mem->mem_page(0)->get_record(k);
                       int hash=r.probe_hash()%(MEM_SIZE_IN_PAGE-2)+1;
                       /*==*/
                       for (int t=0;t<mem->mem_page(hash)->size();t++){
                           if (r==mem->mem_page(hash)->get_record(t)){
                               /*put it in the output page*/
                               /*if the output page is full*/
                               if (mem->mem_page(MEM_SIZE_IN_PAGE-1)->full()){
                                   result.push_back(mem->flushToDisk(disk, MEM_SIZE_IN_PAGE-1));
                                   mem->mem_page(MEM_SIZE_IN_PAGE-1)->reset();
                               }
                               /*...*/
                               mem->mem_page(MEM_SIZE_IN_PAGE-1)->loadPair(mem->mem_page(hash)->get_record(t), r);
                           }
                       }
                   }
               }
               
               for (int n=1;n<=MEM_SIZE_IN_PAGE-2;n++){
                   mem->mem_page(n)->reset();
               }
           
           }
           /*in the end*/
         if (mem->mem_page(MEM_SIZE_IN_PAGE-1)->size()>0){
             result.push_back(mem->flushToDisk(disk, MEM_SIZE_IN_PAGE-1));}
        
        
    }
    return result;

}



