package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"sort"
	"strings"
	"time"

	"fmt"

	"github.com/boltdb/bolt"
	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/heketi/pkg/utils"
)

type Entry struct {
	State api.EntryState
}

type DeviceEntry struct {
	Entry

	Info       api.DeviceInfo
	Bricks     sort.StringSlice
	NodeId     string
	ExtentSize uint64
}

type BrickEntry struct {
	Info             api.BrickInfo
	TpSize           uint64
	PoolMetadataSize uint64
	gidRequested     int64
}

func main() {
	db, err := bolt.Open("heketi.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("hack_heketi starting")

	db.Update(func(tx *bolt.Tx) error {
		device_bucket := tx.Bucket([]byte("DEVICE"))
		brick_bucket := tx.Bucket([]byte("BRICK"))

		c := device_bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {

			if strings.HasPrefix(string(k[:]), "DEVICE") != true {

				entry := DeviceEntry{}

				dec := gob.NewDecoder(bytes.NewReader(v))
				err := dec.Decode(&entry)
				if err != nil {
					fmt.Printf("%s", err)
					return err
				}

				fmt.Printf("Device %s, total_size: %d, free: %d\n", k, entry.Info.Storage.Total, entry.Info.Storage.Free)
				fmt.Println("----------------------------------------")

				if entry.Bricks == nil {
					entry.Bricks = make(sort.StringSlice, 0)
				}

				var bricksToRemove = make(sort.StringSlice, 0)

				var brickTotalSpaceUsed uint64

				fmt.Printf("Node ID is %s\n", entry.NodeId)
				fmt.Printf("Bricks are: [")
				var comma string
				for _, id := range entry.Bricks {
					fmt.Printf("%s%s", comma, id)
					value := brick_bucket.Get([]byte(id))
					if value == nil {
						fmt.Printf("(missing)")
						bricksToRemove = append(bricksToRemove, id)
						continue
					}

					brickEntry := BrickEntry{}
					dec := gob.NewDecoder(bytes.NewReader(value))
					err := dec.Decode(&brickEntry)
					if err != nil {
						fmt.Printf("%s", err)
						return err
					}
					brickTotalSpaceUsed += brickEntry.TpSize

					comma = ", "
				}
				fmt.Printf("]\n")

				for _, id := range bricksToRemove {
					fmt.Printf("removing %s\n", id)
					entry.Bricks = utils.SortedStringsDelete(entry.Bricks, id)
				}

				var calculatedFreeSpace uint64 = entry.Info.Storage.Total - brickTotalSpaceUsed

				fmt.Printf("Total space used %d.\n", brickTotalSpaceUsed)
				fmt.Printf("Free space %d vs %d\n\n", entry.Info.Storage.Free, calculatedFreeSpace)

				entry.Info.Storage.Free = calculatedFreeSpace
				entry.Info.Storage.Used = brickTotalSpaceUsed

				// save the value back
				var buffer bytes.Buffer
				enc := gob.NewEncoder(&buffer)
				err = enc.Encode(&entry)

				if err != nil {
					fmt.Printf("%s", err)
					return err
				}

				//fmt.Printf("Marshalled data is %s", buffer.Bytes())
				device_bucket.Put(k, buffer.Bytes())
			}
		}

		return nil
	})

	defer db.Close()
}
