package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/tealeg/xlsx"
)

type field struct {
	ID       int    `json:"id"`
	Code     string `json:"code"`
	Ccy      string `json:"ccy"`
	CcyNmRU  string `json:"ccyNm_RU"`
	CcyNmUZ  string `json:"ccyNm_UZ"`
	CcyNmUZC string `json:"ccyNm_UZC"`
	CcyNmEN  string `json:"ccyNm_EN"`
	Nominal  string `json:"nominal"`
	Rate     string `json:"rate"`
	Diff     string `json:"diff"`
	Date     string `json:"Date"`
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

	var wg sync.WaitGroup

	errCh := make(chan error, len(fields))

	for _, rate := range fields {
		wg.Add(1)
		go func(rate field) {
			defer wg.Done()

			date, err := time.Parse("02.01.2006", rate.Date)
			if err != nil {
				errCh <- err
				return
			}

			_, err = db.Exec(`
				INSERT INTO json (
					id, code, ccy, ccyNm_RU, ccyNm_UZ, ccyNm_UZC, ccyNm_EN, nominal, rate, diff, Date
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
				rate.ID, rate.Code, rate.Ccy, rate.CcyNmRU, rate.CcyNmUZ, rate.CcyNmUZC, rate.CcyNmEN, rate.Nominal, rate.Rate, rate.Diff, date)
			if err != nil {
				errCh <- err
				return
			}
		}(rate)
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		if err != nil {
			log.Fatal(err)
		}
	}

	err = exportToExcel(fields, "rates.xlsx")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("All data inserted ")
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

func exportToExcel(data []field, filename string) error {
	file := xlsx.NewFile()
	dataMap := make(map[string][]field)

	for _, rate := range data {
		dataMap[rate.Date] = append(dataMap[rate.Date], rate)
	}

	for date, rates := range dataMap {
		sheet, err := file.AddSheet(date)
		if err != nil {
			return err
		}

		headerRow := sheet.AddRow()
		headerRow.AddCell().SetValue("ID")
		headerRow.AddCell().SetValue("Code")
		headerRow.AddCell().SetValue("Currency")
		headerRow.AddCell().SetValue("Nominal")
		headerRow.AddCell().SetValue("Rate")
		headerRow.AddCell().SetValue("Date")

		for _, rate := range rates {
			row := sheet.AddRow()
			row.AddCell().SetValue(rate.ID)
			row.AddCell().SetValue(rate.Code)
			row.AddCell().SetValue(rate.Ccy)
			row.AddCell().SetValue(rate.Nominal)
			row.AddCell().SetValue(rate.Rate)
			row.AddCell().SetValue(rate.Date)
		}
	}

	err := file.Save(filename)
	if err != nil {
		return err
	}

	fmt.Printf("Successfully exported to %s\n", filename)
	return nil
}
