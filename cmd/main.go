package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

var sql = `CREATE TABLE IF NOT EXISTS streaminsert (
  id Int64,
  name String,
  age Int32,
  action String
) Engine = MergeTree()
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part=0,
  index_granularity = 8192,
  write_ahead_log_max_bytes = 8192
`

func main() {
	ctx := context.Background()
	conn, err := ch.Dial(ctx, ch.Options{Address: "localhost:9000"})
	if err != nil {
		panic(err)
	}

	if err := conn.Do(ctx, ch.Query{
		Body: sql,
	}); err != nil {
		panic(err)
	}

	id := new(proto.ColInt64)
	name := new(proto.ColStr)
	age := new(proto.ColInt32)
	action := new(proto.ColStr)

	input := proto.Input{
		{Name: "id", Data: id},
		{Name: "name", Data: name},
		{Name: "age", Data: age},
		{Name: "action", Data: action},
	}

	buf := bufio.NewReader(os.Stdin)
	var blocks int
	cnt := 0
	if err := conn.Do(ctx, ch.Query{
		Body:  input.Into("streaminsert"),
		Input: input,

		OnInput: func(ctx context.Context) error {
			fmt.Printf("wait until something input: ")
			buf.ReadBytes('\n')
			input.Reset()

			if blocks >= 3 {
				return io.EOF
			}

			for i := 0; i < 50000; i++ { // 1M every block
				id.Append(int64(cnt))
				name.Append("PPPP")
				age.Append(rand.Int31())
				action.Append("Test")
				cnt++
			}

			blocks++
			fmt.Printf("start streaming block\n")
			return nil
		},
	}); err != nil {
		panic(err)
	}
	fmt.Printf("Done\n")
	conn.Close()
}
