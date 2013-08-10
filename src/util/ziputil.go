package main

import (
	"archive/zip"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
}

const (
	INPUT_USAGE  = "[mandatory] semi-colon separated list of input files"
	OUTPUT_USAGE = "[mandatory] an output zip file"
	MODE         = "[optional] operation mode. values are a|w append or overwrite"
)

// flags
var files = flag.String("i", "", INPUT_USAGE)
var output = flag.String("o", "", OUTPUT_USAGE)
var mode = flag.String("m", "a", MODE)

func init() {
	flag.Parse()
}

func main() {
	// Need mandatory flags, else print usage
	if strings.EqualFold(*files, "") || strings.EqualFold(*output, "") {
		Usage()
	}

	// A way to visit each flag (includes specificed and unspecified)
	flag.Visit(func(flag *flag.Flag) {
	})

	fmt.Println("Zip the following files ", *files, "to ",
		*output, " using mode ", *mode)

	// Input files to zip
	fileArray := strings.Split(*files, ";")
	fmt.Println("Number of input files ", len(fileArray))

	// Create a Zip Writer around a byte buffer
	buf := new(bytes.Buffer)
	zipWriter := zip.NewWriter(buf)

	// For each input file
	for i := 0; i < len(fileArray); i++ {
		fmt.Println("Opening file ", fileArray[i])

		// Open it
		file, err := os.Open(fileArray[i])
		if err != nil {
			log.Fatal(err)
		}

		// Get its size
		stat, err := file.Stat()
		fmt.Println("File zize of ", fileArray[i], " is ", stat.Size())

		// Allocate an internal buffer
		fileBuff := make([]byte, stat.Size())

		// Read contents into internal buffer
		var nbytes int
		nbytes, err = file.Read(fileBuff)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Bytes read to internal byte array ", nbytes)

		// Create a zip file
		fmt.Println("Zip file name is ", fileArray[i])
		zipFile, err := zipWriter.Create(fileArray[i])
		if err != nil {
			log.Fatal(err)
		}

		// Write contents to the file
		nbytes, err = zipFile.Write(fileBuff)
		fmt.Println("Bytes written ", nbytes)
		if err != nil {
			log.Fatal(err)
		}
	}

	// All files written. Close the zipWriter
	err := zipWriter.Close()
	if err != nil {
		log.Fatal(err)
	}

	// Create the output Zip file
	outputZip, err := os.Create(*output)
	if err != nil {
		log.Fatal(err)
	}

	// Write contents
	outputZip.Write(buf.Bytes())
	err = outputZip.Close()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Done")
}
