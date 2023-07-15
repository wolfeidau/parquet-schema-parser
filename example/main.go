package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow/go/v13/parquet/schema"
	parquetschemaparser "github.com/wolfeidau/parquet-schema-parser"
)

var (
	schemaPath = flag.String("schema", "", "Path to Parquet schema file")
)

func main() {
	flag.Parse()

	if *schemaPath == "" {
		fmt.Fprintln(os.Stderr, "Must specify -schema")
		flag.PrintDefaults()
		os.Exit(1)
	}

	log.SetFlags(log.Ltime | log.Lshortfile)

	rdr, err := os.Open(*schemaPath)
	if err != nil {
		log.Fatalf("failed to read file: %s", err)
	}
	defer rdr.Close()

	ps, err := parquetschemaparser.ParseSchema(rdr)
	if err != nil {
		log.Fatalf("failed to parse file: %s", err)
	}

	schema.PrintSchema(ps, os.Stdout, 2)
}
