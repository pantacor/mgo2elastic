# mgo2elastic - migrate mongo collections to elastic

mgo2elastic makes it trivial to import mongo collections into elasticsearch indices.

# Usage

```
mgo2elastic --help
NAME:
   mgo2elastic - import mongo collections into elastic

USAGE:
   mgo2elastic.exe [global options] command [command options]
   mgo2elastic [-f|--from <MONGO_URL>] -t|--to <ELASTIC_URL> -i|--index <ELASTIC_INDEX>

VERSION:
   0.0.0

COMMANDS:
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   -f MONGO_URL, --from MONGO_URL           The MONGO_URL pointing to the db to export for import into elastic (default: "mongodb://localhost") [%M2E_MONGO_URL%]
   -c COLLECTION, --collection COLLECTION   The COLLECTION pointing to the collection to export for import into elastic (default: "default") [%M2E_COLLECTION%]
   -t ELASTIC_URL, --to ELASTIC_URL         The ELASTIC_URL to import to (default: "http://localhost:9200") [%M2E_ELASTIC_URL%]
   -i ELASTIC_INDEX, --index ELASTIC_INDEX  The ELASTIC_INDEX to import into to. Is a golang template with sprig template func (-> https://github.com/Masterminds/sprig) (default: "mgo2elastic-{{ now |
date \"2006-01-02\" }}") [%M2E_ELASTIC_INDEX%]
   --threads THREADS                        The amount of THREADS (aka goroutine) works to spawn for parallel import into elastic (default: 2) [%M2E_THREADS%]
   --type ELASTIC_TYPE                      The ELASTIC_TYPE used for indexing the documents. default is collection name [%M2E_ELASTIC_TYPE%]
   --help, -h                               show help
   --version, -v                            print the version
```


# Example

Here we import from our mongodb in DB xyz the collection abc into elastic and use a date index as well a type from a field in mgo collection. 

```
$ mgoelastic -f http://localhost:7200/mongo-db \
    -c ourcollection \
    -t http://elastic.url.tld:9200 \
    -i 'ourindex-{{ now | date "2000-01-01" }}' \
    --type "{{ .doc.myfield }}" \
    --threads "5"
```
