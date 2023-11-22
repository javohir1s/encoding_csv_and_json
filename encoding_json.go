package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"
)

type field struct {
	ID       int     `json:"id"`
	Code     string  `json:"code"`
	Ccy      string  `json:"ccy"`
	CcyNmRU  string  `json:"ccyNm_RU"`
	CcyNmUZ  string  `json:"ccyNm_UZ"`
	CcyNmUZC string  `json:"ccyNm_UZC"`
	CcyNmEN  string  `json:"ccyNm_EN"`
	Nominal  string  `json:"nominal"`
	Rate     string  `json:"rate"`
	Diff     string  `json:"diff"`
	Date     string  `json:"Date"`
}

func main() {
	jsonURL := "https://cbu.uz/uz/arkhiv-kursov-valyut/json/"

	jsonData, err := DoRequest(jsonURL, "GET", nil)
	if err != nil {
		log.Fatal(err)
	}

	var fields []field
	err = json.Unmarshal(jsonData, &fields)
	if err != nil {
		log.Fatal(err)
	}

	db, err := sql.Open("postgres", "user=javohir dbname=json password=12345 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = insertDataIntoSQLTable(db, fields)
	if err != nil {
		log.Fatal(err)
	}
}

func insertDataIntoSQLTable(db *sql.DB, data []field) error {
	for _, rate := range data {
		date, err := time.Parse("02.01.2006", rate.Date)
		if err != nil {
			return err
		}

		_, err = db.Exec(`
			INSERT INTO json (
				id, code, ccy, ccyNm_RU, ccyNm_UZ, ccyNm_UZC, ccyNm_EN, nominal, rate, diff, Date
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			rate.ID, rate.Code, rate.Ccy, rate.CcyNmRU, rate.CcyNmUZ, rate.CcyNmUZC, rate.CcyNmEN, rate.Nominal, rate.Rate, rate.Diff, date)
		if err != nil {
			return err
		}
	}

	fmt.Println("successfully insert to sql")
	return nil
}

func DoRequest(url string, method string, body interface{}) ([]byte, error) {
	data, err := json.Marshal(&body)
	if err != nil {
		return nil, err
	}
	client := &http.Client{
		Timeout: time.Duration(20 * time.Second),
	}

	request, err := http.NewRequest(method, url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respByte, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return respByte, nil
}
