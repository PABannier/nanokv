#!/bin/bash

# PUT
dd if=/dev/urandom of=sample.bin bs=1M count=10
curl -v -X PUT --data-binary @sample.bin http://127.0.0.1:8080/mykey

# GET
curl -v http://127.0.0.1:8080/mykey -o out.bin
cmp sample.bin out.bin && echo "OK"

# DELETE
curl -v -X DELETE http://127.0.0.1:8080/mykey
