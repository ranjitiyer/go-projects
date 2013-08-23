package main

import (
	"io/ioutil"
	"fmt"
)

func main() {
	dir := "\\\\narmada\\data\\rest_data\\SDs\\sanitySDs"
	fileInfo, err := ioutil.ReadDir(dir)
	if err != nil {
		fmt.Println(err)
	}

	for _, v := range fileInfo {
		fmt.Printf("%s\\%s", dir, v.Name())
		fmt.Println()
	}

}
