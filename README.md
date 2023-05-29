# Toy application: Web API to query and download data of local filesystem

```
git submodule update --init
cargo run
```


## Example

```bash
curl -X GET --location "http://localhost:3000/download" \
    -H "Content-Type: application/json" \
    -d "{
          \"input_tables\": {
            \"t\": \"./arrow-testing/data/csv/aggregate_test_100.csv\"
          },
          \"sql\": \"select c1, avg(c2), sum(c3) from t group by c1 order by c1\",
          \"output_type\": \"TSV\"
        }"
```
