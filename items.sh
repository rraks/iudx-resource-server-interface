curl -XGET "https://$1/catalogue/v1/search?attribute-filter=(id)" | python -m json.tool > items.json
