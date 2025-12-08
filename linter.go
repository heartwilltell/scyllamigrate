package main

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	fset := token.NewFileSet()
	issues := 0

	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasSuffix(path, ".go") {
			_, err := parser.ParseFile(fset, path, nil, parser.AllErrors)
			if err != nil {
				fmt.Printf("Error in file %s: %s\n", path, err.Error())
				issues++
			}
		}
		return nil
	})

	if err != nil {
		fmt.Printf("Error walking the path: %s\n", err.Error())
		os.Exit(1)
	}

	if issues > 0 {
		fmt.Printf("\nFound %d issues.\n", issues)
		os.Exit(1)
	} else {
		fmt.Println("No issues found.")
	}
}
