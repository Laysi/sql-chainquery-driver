package sql_chainquery_driver

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"github.com/huandu/go-sqlbuilder"
	"github.com/xwb1989/sqlparser"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

type ChainqueryDriver struct {
}

func (d ChainqueryDriver) Open(dsn string) (driver.Conn, error) {
	return &chainqueryConn{
		server: dsn,
		client: &http.Client{},
	}, nil
}

type chainqueryConn struct {
	server string
	client *http.Client
}

type ChainqueryResult struct {
	Success bool
	Error   string
	Data    []map[string]interface{}
}

func (c chainqueryConn) Query(sqlQuery string, args []driver.Value) (*ChainqueryResult, error) {
	iArgs := []interface{}{}
	for _, arg := range args {
		iArgs = append(iArgs, arg)
	}
	sqlQuery, err := sqlbuilder.MySQL.Interpolate(sqlQuery, iArgs)
	if err != nil {
		return nil, err
	}
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
		return nil, errors.New("request failed with [" + resp.Status + "]")
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
	return 0 //figure out what the heck is this
}

func (c *chainqueryStmt) Exec(args []driver.Value) (driver.Result, error) {
	panic("not support write operation")
}

func (c *chainqueryStmt) Query(args []driver.Value) (driver.Rows, error) {
	result, err := c.conn.Query(c.query, args)
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
		columns: columns,
		index:   0,
		data:    result.Data,
	}, nil
}

type chainqueryRows struct {
	columns []string
	data    []map[string]interface{}
	index   int
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
	switch name {
	case "block_size", "nonce", "version", "version_hex", "block_time":
		value := v.(float64)
		return int(value), nil
	case "created_at", "modified_at":
		value := v.(string)
		time, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return nil, err
		}
		return time, nil

	default:
		return v, nil
	}

}

func init() {
	sql.Register("chainquery", &ChainqueryDriver{})
}
