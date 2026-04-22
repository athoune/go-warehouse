# Warehouse

Append only storage for golang.

## Usage Example

Here's a complete example showing how to use the warehouse module:

```go
package main

import (
	"bytes"
	"fmt"
	"log"

	"github.com/athoune/go-warehouse/warehouse"
)

func main() {
	// Create a new warehouse in a temporary directory
	w, err := warehouse.New("/tmp/my-warehouse")
	if err != nil {
		log.Fatal(err)
	}

	// Create a read-write transaction
	tx, err := w.Transaction()
	if err != nil {
		log.Fatal(err)
	}

	// Store some data
	if err := tx.Put([]byte("key1"), []byte("value1")); err != nil {
		log.Fatal(err)
	}
	if err := tx.Put([]byte("key2"), []byte("value2")); err != nil {
		log.Fatal(err)
	}

	// Close the transaction to commit changes
	if err := tx.Close(); err != nil {
		log.Fatal(err)
	}

	// Close the warehouse
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}

	// Reopen in read-only mode to read the data
	wr, err := warehouse.OpenReadOnly("/tmp/my-warehouse")
	if err != nil {
		log.Fatal(err)
	}
	defer wr.Close()

	// Create a read-only transaction
	txr, err := wr.Transaction()
	if err != nil {
		log.Fatal(err)
	}
	defer txr.Close()

	// Read the data back
	buf := &bytes.Buffer{}
	n, err := txr.Read([]byte("key1"), buf)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Read %d bytes: %s\n", n, buf.String())

	// Dump all keys (for debugging)
	if err := txr.Dump(buf); err != nil {
		log.Fatal(err)
	}
}
```

## Features

- **Append-only storage**: Data is written to compressed tablets, never modified in place
- **Transactional**: ACID transactions powered by BoltDB
- **Compression**: Uses zstd for efficient storage
- **Seekable reads**: Random access to compressed data
- **Read-only mode**: Open existing warehouses without write permissions

## Installation

```bash
go get github.com/athoune/go-warehouse/warehouse
```
