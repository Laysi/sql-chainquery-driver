# sql-chainquery-driver
A very restricted sql query driver converter for lbry chainquery public http api endpoint

```go
package main
import (
	"fmt"
	_ "github.com/Laysi/sql-chainquery-driver"
	"github.com/jmoiron/sqlx"
    "time"
)
type Block struct {
	Bits                  string    `db:"bits"`
	BlockSize             int       `db:"block_size"`
	BlockTime             int       `db:"block_time"`
	Chainwork             string    `db:"chainwork"`
	Confirmations         int       `db:"confirmations"`
	CreatedAt             time.Time `db:"created_at"`
	Difficulty            string    `db:"difficulty"`
	Hash                  string    `db:"hash"`
	Height                int       `db:"height"`
	ID                    int       `db:"id"`
	MerkleRoot            string    `db:"merkle_root"`
	ModifiedAt            time.Time `db:"modified_at"`
	NameClaimRoot         string    `db:"name_claim_root"`
	NextBlockHash         *string   `db:"next_block_hash"`
	Nonce                 int       `db:"nonce"`
	PreviousBlockHash     string    `db:"previous_block_hash"`
	TransactionHashes     string    `db:"transaction_hashes"`
	TransactionsProcessed int       `db:"transactions_processed"`
	Version               int       `db:"version"`
	VersionHex            int       `db:"version_hex"`
}


func main() {
	dbConn, err := sqlx.Connect("chainquery", "url(https://chainquery.lbry.com)/")
	if err != nil {
		panic(err)
		return
	}
	testData := []Block{}

	err = dbConn.Select(&testData, "SELECT * FROM block ORDER BY height DESC LIMIT 1")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v", testData)
}
```