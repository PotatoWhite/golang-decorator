package main

import (
	"fmt"
	"golang-decorator/simple/simeplestruct"
	"log"
)

func main() {
	m := simeplestruct.NewSimpleStruct([]string{"localhost:9092"}, "myTopic")

	if result, err := m.Test1AndPub(1, "test", "test"); err != nil {
		log.Fatalf("Test1AndPub failed: %s", err)
	} else {
		fmt.Println("Test1AndPub result:", result)
	}

	if result1, result2, err := m.Test2AndPub(1, []int{1, 2, 3}, "test", "test"); err != nil {
		log.Fatalf("Test2AndPub failed: %s", err)
	} else {
		fmt.Println("Test2AndPub results:", result1, result2)
	}
}
