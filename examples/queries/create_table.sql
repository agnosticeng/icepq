select icepq_create_table(
    'table01',
    '{
        "table_location": "s3://test01/table01",
        "schema": [
            {
                "id": 1,
                "name": "title",
                "type": "string",
                "required: "true"
            }
        ],
        "sort_order" : {
            "order-id": 1,
            "fields": [
                "source-id": 1,
                "transform": "identity",
                "direction: "asc",
                "null-order": "nulls-last"
            ]
        },
        "partition_spec": [],
        "properties": {}
    }'
)