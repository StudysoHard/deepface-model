import random
from pymilvus_orm import connections, FieldSchema, CollectionSchema, DataType, Collection,  utility
import time
import calendar
from deepface.milvus import snowFlow,OperationMysql


# This example shows how to:
#   1. connect to Milvus server
#   2. create a collection
#   3. insert entities
#   4. create index
#   5. search


_HOST = '127.0.0.1'
_PORT = '19530'

# Const names
_COLLECTION_NAME = 'face_milvus'
_ID_FIELD_NAME = 'id_field'
_VECTOR_FIELD_NAME = 'face_vector'

# Vector parameters
_DIM = 2622
_INDEX_FILE_SIZE = 32  # max file size of stored index

# Index parameters
_METRIC_TYPE = 'L2'
_INDEX_TYPE = 'IVF_FLAT'
_NLIST = 1024
_NPROBE = 16
_TOPK = 1


# Create a Milvus connection
def create_connection():
    print(f"\nCreate connection...")
    connections.connect(host=_HOST, port=_PORT)
    print(f"\nList connections:")
    print(connections.list_connections())


# 不存在就创建collection
def boolean_collection():
    if (has_collection(_COLLECTION_NAME) == False):
        return create_collection(_COLLECTION_NAME, _ID_FIELD_NAME, _VECTOR_FIELD_NAME)
    else :
        field1 = FieldSchema(name=_ID_FIELD_NAME, dtype=DataType.INT64, description="int64", is_primary=True)
        field2 = FieldSchema(name=_VECTOR_FIELD_NAME, dtype=DataType.FLOAT_VECTOR, description="float vector", dim=_DIM,
                             is_primary=False)
        schema = CollectionSchema(fields=[field1, field2], description="collection description")
        return Collection(name=_COLLECTION_NAME, schema=schema)

# Create a collection named 'demo'
def create_collection(name, id_field, vector_field):
    field1 = FieldSchema(name=id_field, dtype=DataType.INT64, description="int64", is_primary=True)
    field2 = FieldSchema(name=vector_field, dtype=DataType.FLOAT_VECTOR, description="float vector", dim=_DIM,
                         is_primary=False)
    schema = CollectionSchema(fields=[field1, field2], description="collection description")
    collection = Collection(name=name, data=None, schema=schema)
    print("\ncollection created:", name)
    return collection


def has_collection(name):
    return utility.has_collection(name)


# Drop a collection in Milvus
def drop_collection(name):
    collection = Collection(name)
    collection.drop()
    print("\nDrop collection: {}".format(name))


# List all collections in Milvus
def list_collections():
    print("\nlist collections:")
    print(utility.list_collections())

def insert(collection, dim , human_face):
    # 当前时间错
    ts = calendar.timegm(time.gmtime())
    # 雪花算法的id
    snowId =  snowFlow.IdWorker(1, 2, 0)
    milvus_id = snowId.get_id()
    # 插入milvus的数据
    data = [
        [milvus_id],
        # [[random.random() for _ in range(dim)]],
        [human_face],
    ]
    op_mysql = OperationMysql.OperationMysql()
    res = op_mysql.insert_one(milvus_id,2,ts)

    collection.insert(data)


def get_entity_num(collection):
    print("\nThe number of entity:")
    print(collection.num_entities)


def create_index(collection, filed_name):
    index_param = {
        "index_type": _INDEX_TYPE,
        "params": {"nlist": _NLIST},
        "metric_type": _METRIC_TYPE}
    collection.create_index(filed_name, index_param)
    print("\nCreated index:\n{}".format(collection.index().params))


def drop_index(collection):
    collection.drop_index()
    print("\nDrop index sucessfully")


def release_collection(collection):
    collection.release()


def search(collection, vector_field, id_field, search_vectors):
    search_param = {
        "data": [search_vectors],
        "anns_field": vector_field,
        "param": {"metric_type": _METRIC_TYPE, "params": {"nprobe": _NPROBE}},
        "limit": _TOPK,
        "expr": "id_field > 0"}
    results = collection.search(**search_param)


    for i, result in enumerate(results):
        print("\nSearch result for {}th vector: ".format(i))
        for j, res in enumerate(result):
            print("Top {}: {}".format(j, res))
            faceVector = collection.query(
                expr="id_field in ["+str(res.id)+"]",
                output_fields=["face_vector"]
            )
            return faceVector[0]['face_vector']


def insertAndFindMilvus(human_face):
    # create a connection
    create_connection()
    # find collection is created ?
    collection =  boolean_collection()

    collection.load()
    if(collection.has_index() == False):
        create_index(collection,_VECTOR_FIELD_NAME)

    # search face
    dataFace = search(collection, _VECTOR_FIELD_NAME, _ID_FIELD_NAME, human_face)

    # insert face
    insert(collection, _DIM, human_face)

    release_collection(collection)
    return dataFace


# def main():


# create collection
    # collection = create_collection(_COLLECTION_NAME, _ID_FIELD_NAME, _VECTOR_FIELD_NAME)
    #
    # # show collections
    # list_collections()
    #
    # # insert 10000 vectors with 128 dimension
    # vectors = insert(collection, _DIM)
    #
    # # get the number of entities
    # get_entity_num(collection)
    #
    # # create index
    # create_index(collection, _VECTOR_FIELD_NAME)
    #
    # # load data to memory
    # load_collection(collection)
    #
    # # search
    # search(collection, _VECTOR_FIELD_NAME, _ID_FIELD_NAME, vectors[:3])

    # drop collection index
    # drop_index(collection)

    # release memory
    # release_collection(collection)

    # drop collection
    # drop_collection(_COLLECTION_NAME)
