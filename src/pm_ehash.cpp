#include"pm_ehash.h"
#include<cstdio>
#include <windows.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <io.h>
#include <string.h>
#include <libpmem.h>

/**
 * @description: construct a new instance of PmEHash in a default directory
 * @param NULL
 * @return: new instance of PmEHash
 */
PmEHash::PmEHash() {
	PATH = PM_EHASH_DIRECTORY + METADATA_NAME;
	if (open(PATH,O_RDWR)) //数据文件夹下有旧哈希则调用recover函数 
		recover();
	else {	
		int is_pmem = 0;
		uint64_t len;
	
		//新建元数据文件
		metadata = new ehash_metadata();
		
		//初始化元数据文件 
		metadata->max_file_id = 2; //数据页文件码起始值为1，所以相邻值设为2 
		metadata->catalog_size = DEFAULT_CATALOG_SIZE;
		metadata->global_depth = 4 
		
		//映射
		void* metadata_File = pmem_map_file(PATH, (metadata->catalog_size) * sizeof(pm_bucket), PMEM_FILE_CREATE, 0666, &len, &is_pmem); 
		 
		 //flush
		if (is_pmem)
			pmem_persist(metadat_file,len);
		else pmem_msync(metadat_file,len);
		
		//新建目录
		catalog = new ehash_catalog();
		
		//初始化目录 
		catalog->buckets_pm_address = new pm_address[DEFAULT_CATALOG_SIZE];
		catalog->buckets_virtual_address = new pm_bucket*[DEFAULT_CATALOG_SIZE];
		
		//映射
		PATH = PM_EHASH_DIRECTORY + CATALOG_NAME;
		void *catalog_File = pmem_map_file(PATH,  (metadata->catalog_size) * sizeof(catalog), PMEM_FILE_CREATE, 0666, &len, &is_pmem);
		
		//flush
		if (is_pmem)
			pmem_persist(catalog_file,len);
		else pmem_msync(catalog_file,len);
	} 
}
/**
 * @description: persist and munmap all data in NVM
 * @param NULL
 * @return: NULL
 */
PmEHash::~PmEHash() {
	int is_pmem = 0;
	uint64_t len;
	
	//持久化metadata 
	PATH = PM_EHASH_DIRECTORY + METADATA_NAME;
	void* metadata_File = pmem_map_file(PATH, (metadata->catalog_size) * sizeof(pm_bucket), PMEM_FILE_CREATE, 0666, &len, &is_pmem); 
	if (is_pmem)
		pmem_persist(metadat_file,len);
	else pmem_msync(metadat_file,len);
	
	//持久化目录 
	PATH = PM_EHASH_DIRECTORY + CATALOG_NAME;
	void *catalog_File = pmem_map_file(PATH, (metadata->catalog_size) * sizeof(catalog) , PMEM_FILE_CREATE, 0666, &len, &is_pmem);
	if (is_pmem)
		pmem_persist(catalog_file,len);
	else pmem_msync(catalog_file,len);
	
	//解除映射
	pmem_unmap(metadata_File,len);
	pmem_unmap(catalog_File,len); 
}

/**
 * @description: 插入新的键值对，并将相应位置上的位图置1
 * @param kv: 插入的键值对
 * @return: 0 = insert successfully, -1 = fail to insert(target data with same key exist)
 */
int PmEHash::insert(kv new_kv_pair) {
	uint64_t tmp = 0;
	if (search(new_kv_pair.key, tmp) == 0) return -1;
	pm_bucket *bucket = getFreeBucket(new_kv_pair.key);
	kv *freePlace = getFreeKvSlot(bucket);
	*freePlace = new_kv_pair;
	size_t i;
	for (i = 0; i <  BUCKET_SLOT_NUM; i++) {
		if (&(bucket->slot[i]) == freePlace)
			bucket->bitmap[i / 8 + 1] = 1;
	}
	pmem_persit(freePlace,sizeof(freePlace);
	return 0;
}

/**
 * @description: 删除具有目标键的键值对数据，不直接将数据置0，而是将相应位图置0即可
 * @param uint64_t: 要删除的目标键值对的键
 * @return: 0 = removing successfully, -1 = fail to remove(target data doesn't exist)
 */
int PmEHash::remove(uint64_t key) {
	uint64_t tmp;
	if (search(key, tmp) == -1)return -1;
	uint64_t id = hashFunc(key);
	pm_bucket *bucket = catalog.buckets_virtual_address[id];
	size_t i;
	for (i = 0; i < BUCKET_SLOT_NUM; i++) {
		if (bucket->slot[i].key == key) {
			bucket->bitmap[i / 8 + 1] = 0;
			if (i == 0) {
				mergeBucket(id);
			}
			return 0;
		}
	}
}
/**
 * @description: 更新现存的键值对的值
 * @param kv: 更新的键值对，有原键和新值
 * @return: 0 = update successfully, -1 = fail to update(target data doesn't exist)
 */
int PmEHash::update(kv kv_pair) {
	uint64_t tmp;
	if (search(kv_pair.key, tmp) == -1) return -1;
	uint64_t id = hashFunc(kv_pair.key);
	pm_bucket *bucket = catalog.buckets_virtual_address[id];
	uint64_t i;
	for (i = 0; i < BUCKET_SLOT_NUM; i++) {
		if (bucket->slot[i].key == kv_pair.key) {
			bucket->slot[i].value = kv_pair.value;
			return 0;
		}
	}
}
/**
 * @description: 查找目标键值对数据，将返回值放在参数里的引用类型进行返回
 * @param uint64_t: 查询的目标键
 * @param uint64_t&: 查询成功后返回的目标值
 * @return: 0 = search successfully, -1 = fail to search(target data doesn't exist)
 */
int PmEHash::search(uint64_t key, uint64_t& return_val) {
	uint64_t id = hashFunc(key);
	pm_bucket *bucket = catalog.buckets_virtual_address[id];
	uint64_t i;
	for (i = 0; i < BUCKET_SLOT_NUM; i++) {
		if (bucket->slot[i].key == key) {
			return_val = bucket->slot[i].value;
			return 0;
		}
	}
	return -1;
}

/**
 * @description: 用于对输入的键产生哈希值，然后取模求桶号(自己挑选合适的哈希函数处理)
 * @param uint64_t: 输入的键
 * @return: 返回键所属的桶号
 */
uint64_t PmEHash::hashFunc(uint64_t key) {
	return key & ((1 << metadata->global_depth) - 1);
}

/**
 * @description: 获得供插入的空闲的桶，无空闲桶则先分裂桶然后再返回空闲的桶
 * @param uint64_t: 带插入的键
 * @return: 空闲桶的虚拟地址
 */
pm_bucket* PmEHash::getFreeBucket(uint64_t key) {
	pm_bucket *bucket = catalog.buckets_virtual_address[hashFunc(key)];
	for (uint64_t i = 0; i < BUCKET_SLOT_NUM; i++) {
		if (bucket->bitmap[i / 8 + 1] == 0) 
			return bucket;
	}
	splitBucket(hashFunc(key));
	return bucket;
}

/**
 * @description: 获得空闲桶内第一个空闲的位置供键值对插入
 * @param pm_bucket* bucket
 * @return: 空闲键值对位置的虚拟地址
 */ 
kv* PmEHash::getFreeKvSlot(pm_bucket* bucket) {
	for (uint64_t i = 0; i < BUCKET_SLOT_NUM; i++) {
		if (bucket->bitmap[i / 8 + 1] == 0)
			return &(bucket->slot[i]);
	}
}

/**
 * @description: 桶满后进行分裂操作，可能触发目录的倍增
 * @param uint64_t: 目标桶在目录中的序号
 * @return: NULL
 */
void PmEHash::splitBucket(uint64_t bucket_id) {
	//获得目标桶 
	pm_bucket *bucket = catalog->buckets_virtual_address[bucket_id];
	
	//判断是否需要目录倍增 
	if (bucket->local_depth == metadata->global_depth){
		extendCatalog();
	}
	
	//桶的局部深度加 1 
	uint64_t splited_id = bucket_id + (1 << bucket->local_depth);
	bucket->local_depth ++;
	
	//获得一个新桶 
	pm_bucket *splited = getFreeSlot(pm_address& new_address);
	
	//初始化新桶 splited 
	splited->local_depth = bucket->local_depth;
	for (uint64_t i = 0; i < BUCKET_SLOT_NUM; i ++){
		if (bucket->bitmap[i/8 + 1] == 1){
			splited->slot[i] = bucket->slot[i];
			splited->bitmap[i/8 + 1] = 1;
			bucket->bitmap[i/8 + 1] = 0;
		}
		else splited->bitmap[i/8 + 1] = 0;
	}
	
	//更新目录中的地址映射关系 
	uint64_t num = 1 << splited->local_depth;
	for(uint64_t i = bucket_id + num; i < (1 << global_depth); i += num)
		catalog->buckets_virtual_address[i] = bucket;
	for(uint64_t i = splited_id + num; i < (1 << global_depth); i += num)
		catalog->buckets_virtual_address[i] = splited;
}

/**
 * @description: 桶空后，回收桶的空间，并设置相应目录项指针
 * @param uint64_t: 桶号
 * @return: NULL
 */
void PmEHash::mergeBucket(uint64_t bucket_id) {
	//获得bucket_id对应的桶
	pm_bucket* bucket = catalog.buckets_virtual_address[bucket_id];
	
	//将桶放入free_list
	free_list.push(bucket);
	
	//将对应目录项指向数据页的槽的指针置为空
	catalog.buckets_virtual_address[bucket_id] = NULL;

}

/**
 * @description: 对目录进行倍增，需要重新生成新的目录文件并复制旧值，然后删除旧的目录文件
 * @param NULL
 * @return: NULL
 */
void PmEHash::extendCatalog() {
	
	//全局深度加 1 
	metadata->global_depth++;
	metadata->catalog_size = 1 << metadata->global_depth;
	
	//生成新的目录文件
	ehash_catalog new_catalog = new ehash_catalog();
	new_catalog->buckets_pm_address = new pm_address[DEFAULT_CATALOG_SIZE * 2];
	new_catalog->buckets_virtual_address = new pm_bucket*[DEFAULT_CATALOG_SIZE * 2];
	
	//复制旧值
	memcpy(new_catalog->buckets_pm_address, catalog->buckets_pm_address, sizeof(pm_address) * metadata->catalog_size);
	memcpy(new_catalog->buckets_virtual_address, catalog->buckets_virtual_address, sizeof(pm_bucket *) * metadata->catalog_size);
	
	//删除旧的目录文件 
	delete catalog;
	
	//将新目录文件中内容复制到catalog中
	memcpy(catalog->buckets_pm_address, new_catalog->buckets_pm_address, sizeof(pm_address) * metadata->catalog_size);
	memcpy(catalog->buckets_virtual_address, new_catalog->buckets_virtual_address, sizeof(pm_bucket *) * metadata->catalog_size);
}

/**
 * @description: 获得一个可用的数据页的新槽位供哈希桶使用，如果没有则先申请新的数据页
 * @param pm_address&: 新槽位的持久化文件地址，作为引用参数返回
 * @return: 新槽位的虚拟地址
 */
void* PmEHash::getFreeSlot(pm_address& new_address) {
	//如果没有空闲数据页槽位，则申请新的数据页文件 
	if (free_list.empty())
		allowNewPage();
	
	//获得新槽位的虚拟地址
	pm_bucket* bucket = free_list.front();
	
	//从队列中删除已被使用的虚拟地址 
	free_list.pop();
	//获得新槽位对应的物理地址 
	new_address = vAddr2pmAddr[bucket];
	
	// 返回虚拟地址 
	return bucket;
}

/**
 * @description: 申请新的数据页文件，并把所有新产生的空闲槽的地址放入free_list等数据结构中
 * @param NULL
 * @return: NULL
 */
void PmEHash::allocNewPage() {
	//申请新的数据页，将max_file_id +1；
	//初始化数据页
	//将所有新产生的空闲槽的地址放入free_list等数据结构中
}

/**
 * @description: 读取旧数据文件重新载入哈希，恢复哈希关闭前的状态
 * @param NULL
 * @return: NULL
 */
void PmEHash::recover() {
	int is_pmem = 0;
	uint64_t len;
	
	//读取metadata文件中的数据并内存映射
	PATH =  PM_EHASH_DIRECTORY + METADATA_NAME;
	metadata = pmem_map_file(PATH, (metadata->catalog_size) * sizeof(pm_bucket), PMEM_FILE_CREATE, 0666, &len, &is_pmem);
	
    //读取catalog文件中的数据并内存映射
    PATH = PM_EHASH_DIRECTORY + CATALOG_NAME;
	catalog = pmem_map_file(PATH, (metadata->catalog_size) * sizeof(catalog), PMEM_FILE_CREATE, 0666, &len, &is_pmem);
	
	//读取所有数据页文件并内存映射
    mapAllPage();
    
	//设置可扩展哈希的桶的虚拟地址指针
    for (uint64_t i = 0; i < metadata->catalog_size; i ++){
		pm_address address = catalog->buckets_pm_address[i];
		pmAddr2vAddr[address] = catalog->buckets_virtual_address[i];
	}
    
	//初始化所有其他可扩展哈希的内存数据
	free_list = queue<pm_bucket*>();
}

/**
 * @description: 重启时，将所有数据页进行内存映射，设置地址间的映射关系，空闲的和使用的槽位都需要设置
 * @param NULL
 * @return: NULL
 */
void PmEHash::mapAllPage() {
	int num = metadata->max_file_id;
	string *str;
	for (int i = 0; i < num; i++) {
		str[i] = PM_EHASH_DIRECTORY + i;
	}
	data_page *page;
	for (uint64_t i = 0; i < num; i++) {
		page[i] = pmem_map_file(str[i], sizeof(data_page), PMEM_FILE_CTREATE, 0777, NULL, NULL);//page[i]:指向文件的指针
	}
	catalog.buckets_pm_address = new pm_address[metadata->catalog_size];
	catalog.buckets_virtual_address = new pm_bucket *[metadata->catalog_size];
	for (int i = 0; i < metadata->catalog_size; i++) {
		vAddr2pmAddr[catalog.buckets_virtual_address[i]] = catalog.buckets_pm_address[i];
		pmAddr2vAddr[catalog.buckets_pm_address[i]] = catalog.buckets_virtual_address[i];
	}

}

/**
 * @description: 删除PmEHash对象所有数据页，目录和元数据文件，主要供gtest使用。即清空所有可扩展哈希的文件数据，不止是内存上的
 * @param NULL
 * @return: NULL
 */
void PmEHash::selfDestory() {
	//删除数据页
	string *str;
	for (uint64_t i = 1; i < metadata->max_file_id; i++) {
		str[i] = PM_EHASH_DIRECTORY + i;
		DeleteFile(str[i]);
	}
	
	//删除目录
	pmem_unmap(catalog, sizeof(catalog)); 
	delete catalog;
	
	//删除元数据文件
	 pmem_unmap(metadata, sizeof(metadata));
	 delete metadata;
	 
	 //清空free_list
	free_list = queue<pm_bucket*>();
	  
	  //清空地址映射
	vAddr2pmAddr.clear(); 
	pmAddr2vAddr.clear(); 
}
