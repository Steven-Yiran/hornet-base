package main

import (
	"fmt"
	"math"
)

type Bucket struct {
	depth int64
}

func powInt(x, y int64) int64 {
	return int64(math.Pow(float64(x), float64(y)))
}

func main() {
	// Sample values for hash and bucket's depth
	hash := int64(0b11101101) // Assuming a hash value of 5 for demonstration
	originalDepth := int64(4)
	depth := originalDepth + 1 // Assuming depth of 3 for demonstration

	mask := powInt(2, originalDepth)
	hash = hash % mask

	//depth = depth + 1
	// Computing newHashKey
	//newHashKey := hash + powInt(2, bucket.depth-1)
	newHashKey := hash + powInt(2, depth-1)
	fmt.Printf("Original Hash Key: %04b\n", hash)
	fmt.Printf("New Hash Key: %04b\n", newHashKey)

	hash = hash % powInt(2, 2)
	fmt.Printf("Hash: %04b\n", hash)
}
