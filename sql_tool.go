package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

const (
	CONFIG_PATH      = "./config.json"
	SQL_RECORDS_PATH = "./sql_records.txt"
)

type Mysql struct {
	Endpoint string `json:"endpoint"`
	User     string `json:"user"`
	Password string `json:"password"`
	Schema   string `json:"schema"`
}

type Input struct {
	Mysql           Mysql  `json:"mysql"`
	OriginTable     string `json:"origin_table"`
	TargetTable     string `json:"target_table"`
	Condition       string `json"condition"`
	OriginColumns   string `json:"origin_columns"` // comma
	TargetColumns   string `json:"target_columns"`
	OriginKeyColumn string `json:"origin_key_column"`
	TargetKeyColumn string `json:"target_key_column"`
	Operation       string `json:"operation"`
}

func readJson(confFile string) (*Input, error) {

	bytez, err := ioutil.ReadFile(confFile)
	if err != nil {
		return nil, err
	}
	var input Input
	err = json.Unmarshal(bytez, &input)
	if err != nil {
		return nil, err
	}
	if input.Mysql.Endpoint == "" {
		input.Mysql.Endpoint = "localhost:3306"
	}
	if input.Mysql.User == "" {
		return nil, fmt.Errorf("Empty mysql user")
	}
	if input.OriginKeyColumn != "" {
		if input.TargetKeyColumn == "" {
			input.TargetKeyColumn = input.OriginKeyColumn
		}
	} else {
		return nil, fmt.Errorf("Empty OriginKeyColumn")
	}
	if input.OriginColumns == "" {
		return nil, fmt.Errorf("Empty OriginColumns")
	}
	if input.TargetColumns == "" {
		input.TargetColumns = input.OriginColumns
	}
	if input.OriginColumns != "*" {
		input.OriginColumns = fmt.Sprintf("%s,%s", input.OriginKeyColumn, input.OriginColumns)
	}
	if input.TargetColumns != "*" {
		input.TargetColumns = fmt.Sprintf("%s,%s", input.TargetKeyColumn, input.TargetColumns)
	}
	return &input, nil
}
func executeSql() {

}

func (input *Input) transfer() error {

	mysqlStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?tls=skip-verify&autocommit=true", input.Mysql.User, input.Mysql.Password, input.Mysql.Endpoint, input.Mysql.Schema)
	db, err := sql.Open("mysql", mysqlStr)
	log.Println(mysqlStr, err, db)
	if err != nil {
		return err
	}
	// Query
	var sqlStr string
	if input.Condition != "" {
		sqlStr = fmt.Sprintf("SELECT %s FROM %s WHERE %s", input.OriginColumns, input.OriginTable, input.Condition)
	} else {
		sqlStr = fmt.Sprintf("SELECT %s FROM %s", input.OriginColumns, input.OriginTable)
	}
	originResult, err := queryAndParseRows(db, sqlStr)
	if err != nil {
		return err
	}
	os.Remove(SQL_RECORDS_PATH)
	file, err := os.Create(SQL_RECORDS_PATH)
	if err != nil {
		log.Println(err)
	}
	defer file.Close()
	for i := 0; i < len(originResult); i++ {
		// Insert or Update
		sqlStr = fmt.Sprintf("REPLACE INTO %s(%s) VALUES (%s)", input.TargetTable, input.TargetColumns, originResult[i])
		file.WriteString(fmt.Sprintf("%s\n", sqlStr))
		_, errE := db.Exec(sqlStr)
		if errE != nil {
			log.Println(errE)
			file.WriteString(fmt.Sprintf("######%v,%s\n", errE, sqlStr))
		}
	}
	return nil

}

func queryAndParseRows(Db *sql.DB, queryStr string) ([]string, error) {

	rows, err := Db.Query(queryStr)
	defer rows.Close()
	if err != nil {
		log.Fatalf("查询出错:\nSQL:\n%s, 错误详情:%s\n", queryStr, err.Error())
		return nil, err
	}
	//获取列名cols
	cols, _ := rows.Columns()
	if len(cols) > 0 {
		var ret []string
		for rows.Next() {
			buff := make([]interface{}, len(cols))
			data := make([][]byte, len(cols)) //数据库中的NULL值可以扫描到字节中
			for i, _ := range buff {
				buff[i] = &data[i]
			}
			rows.Scan(buff...) //扫描到buff接口中，实际是字符串类型data中
			//将每一行数据存放到数组中
			dataKv := ""
			for _, col := range data { //k是index，col是对应的值
				dataKv += string(col) + ","
			}
			ret = append(ret, strings.TrimRight(dataKv, ","))
		}
		return ret, nil
	} else {
		return nil, fmt.Errorf("Empty columns")
	}
}
func main() {

	confFile := flag.String("c", CONFIG_PATH, "请输入配置文件地址")
	flag.Parse()
	input, err := readJson(*confFile)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%+v\n", input)
	err = input.transfer()
	log.Println(err)
}
