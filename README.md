# sql-chainquery-driver

```go
import (
	"fmt"
	_ "github.com/Laysi/sql-chainquery-driver"
	"github.com/jmoiron/sqlx"
)

func main() {
	dbConn, err := sqlx.Connect("chainquery", "https://chainquery.lbry.com")
	if err != nil {
		panic(err)
		return
	}
	testData := []Data{}

	err = dbConn.Select(&testData, "SELECT * FROM block ORDER BY height DESC LIMIT 1")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v", testData)
}
```