package sql_chainquery_driver

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"github.com/go-sql-driver/mysql"
	"github.com/huandu/go-sqlbuilder"
	"github.com/spf13/cast"
	"github.com/xwb1989/sqlparser"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type ChainqueryDriver struct {
}

func (d ChainqueryDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	return &chainqueryConn{
		server:       cfg.Addr,
		client:       &http.Client{},
		lastCallTime: time.Now(),
	}, nil
}

type chainqueryConn struct {
	server       string
	client       *http.Client
	lastCallTime time.Time
}

type ChainqueryResult struct {
	Success bool
	Error   string
	Data    []map[string]interface{}
}

//var lastCallTime time.Time = time.Now()
func (c *chainqueryConn) waitFrequencyLimit() {
	since := time.Since(c.lastCallTime)
	//fmt.Printf("chainquery driver called again after %v\n", since)
	if since < time.Millisecond*200 {
		time.Sleep(time.Millisecond*200 - since)
	}
	c.lastCallTime = time.Now()

}
func (c *chainqueryConn) Query(sqlQuery string) (*ChainqueryResult, error) {
	c.waitFrequencyLimit()

	//iArgs := []interface{}{}
	//for _, arg := range args {
	//	iArgs = append(iArgs, arg)
	//}
	//sqlQuery, err := sqlbuilder.MySQL.Interpolate(sqlQuery, iArgs)
	//if err != nil {
	//	return nil, err
	//}
	queryUrl, err := url.Parse(c.server + "/api/sql")
	if err != nil {
		return nil, err
	}
	params := url.Values{}
	params.Add("query", sqlQuery)
	queryUrl.RawQuery = params.Encode()

	req, err := http.NewRequest("GET", queryUrl.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, errors.New("request failed with [" + resp.Status + "] with query [" + sqlQuery + "] url[" + queryUrl.String() + "]")
	}

	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	result := ChainqueryResult{}
	err = json.Unmarshal(bodyBytes, &result)
	if err != nil {
		return nil, nil
	}
	return &result, nil

}

func (c *chainqueryConn) Prepare(query string) (driver.Stmt, error) {
	columns, err := c.extractSelectColumns(query)
	for i, column := range columns {
		columns[i] = strings.ToLower(column)
	}
	if err != nil {
		return nil, err
	}
	return &chainqueryStmt{
		conn:    c,
		query:   query,
		columns: columns,
	}, nil
}

func (c *chainqueryConn) extractSelectColumns(query string) ([]string, error) {
	result := []string{}
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	selectStmt := stmt.(*sqlparser.Select)
	for _, expr := range selectStmt.SelectExprs {
		switch realExpr := expr.(type) {
		case *sqlparser.StarExpr:
			return nil, nil
		case *sqlparser.AliasedExpr:
			if !realExpr.As.IsEmpty() {
				result = append(result, realExpr.As.String())
			} else {
				result = append(result, realExpr.Expr.(*sqlparser.ColName).Name.String())
			}
		}
	}
	return result, nil
}

func (c *chainqueryConn) Close() error {
	return nil
}

func (c *chainqueryConn) Begin() (driver.Tx, error) {
	panic("not support transaction")
}

type chainqueryStmt struct {
	conn    *chainqueryConn
	query   string
	columns []string
}

func (c *chainqueryStmt) Close() error {
	return nil
}

func (c *chainqueryStmt) NumInput() int {
	return -1 //figure out what the heck is this
}

func (c *chainqueryStmt) Exec(args []driver.Value) (driver.Result, error) {
	panic("not support write operation")
}

func (c *chainqueryStmt) Query(args []driver.Value) (driver.Rows, error) {
	iArgs := []interface{}{}
	for _, arg := range args {
		iArgs = append(iArgs, arg)
	}
	sqlQuery, err := sqlbuilder.MySQL.Interpolate(c.query, iArgs)
	if err != nil {
		return nil, err
	}
	result, err := c.conn.Query(sqlQuery)
	if err != nil {
		return nil, err
	}
	columns := []string{}
	if len(c.columns) > 0 {
		columns = c.columns
	} else if len(result.Data) > 0 {
		for key := range result.Data[0] {
			columns = append(columns, key)
		}
	}
	return &chainqueryRows{
		columns:  columns,
		index:    0,
		data:     result.Data,
		rawQuery: sqlQuery,
		query:    c.query,
		//args:args,
	}, nil
}

type chainqueryRows struct {
	columns  []string
	data     []map[string]interface{}
	index    int
	stmt     *chainqueryStmt
	rawQuery string
	query    string
}

func (c *chainqueryRows) Columns() []string {
	return c.columns
}

func (c *chainqueryRows) Close() error {
	return nil
}

func (c *chainqueryRows) Next(dest []driver.Value) error {
	if c.index < len(c.data) {
		row := c.data[c.index]
		for i := range dest {
			value, err := c.typeWorkaround(c.columns[i], row[c.columns[i]])
			if err != nil {
				return err
			}
			dest[i] = value
			if v, ok := dest[i].(string); ok {
				dest[i] = []byte(v)
			}
		}
		c.index += 1
		return nil
	}
	return io.EOF
}
func (c *chainqueryRows) typeWorkaround(name, v interface{}) (interface{}, error) {
	if v == nil {
		return v, nil
	}
	switch name {
	case "block_size", "nonce", "version", "version_hex", "block_time", "id", "effective_amount", "certificate_amount", "frame_width", "frame_height", "duration", "channel_claim_count", "claim_count":
		value := v.(float64)
		return int(value), nil
	case "created_at", "modified_at", "transaction_time", "release_time":
		switch value := v.(type) {
		case string:
			time, err := time.Parse(time.RFC3339, value)
			if err != nil {
				return nil, err
			}
			return time, nil
		case float64:
			return int(value), nil
		default:
			return value, nil
		}
	case "is_cert_valid", "is_nsfw":
		return cast.ToBoolE(v)
	default:
		return v, nil
	}

}

func init() {
	sql.Register("chainquery", &ChainqueryDriver{})
}
