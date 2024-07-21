package hash

func IsHash(index *HashIndex) (bool, error) {
	table := index.GetTable()
	buckets := table.GetBuckets()
	for _, pn := range buckets {
		// Get bucket
		bucket, err := table.GetBucketByPN(pn)
		d := bucket.GetDepth()
		if err != nil {
			return false, err
		}
		// Get all entries
		entries, err := bucket.Select()
		if err != nil {
			return false, err
		}
		// Check that all entries should hash to this bucket.
		for _, e := range entries {
			key := e.GetKey()
			hash := Hasher(key, d)
			if pn != table.buckets[hash] {
				return false, nil
			}
		}
	}
	return true, nil
}
