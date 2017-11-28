# mgo2elastic - migrate mongo collections to elastic

mgo2elastic allows gives you an easy way to import mongo collections
into elastic indexes.

# Usage

Example
```
mgo2elastic --from=mongodb://localhost/mydb/mycollection --find="{}" \
    --to=http://localhost:9200/myindex-{date} \
    --datetmpl='{.field | date }
```