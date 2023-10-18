package main

import (
	"fmt"

	"github.com/XDRAGON2002/consistenthash/pkg/consistenthash"
)

func main() {
	ch := consistenthash.GetNewConsistentHash()

	ch.AddServerWithReplicas("server-1", 3)
	ch.AddServerWithReplicas("server-2", 3)

	ch.AddKey("test", "temp")
	val, _ := ch.GetKey("test")
	fmt.Printf("%+v\n", val)
}
