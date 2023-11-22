package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type User struct {
	ID               uuid.UUID `json:"id"`
	FirstName        string    `json:"first_name"`
	LastName         string    `json:"last_name"`
	Email            string    `json:"email"`
	Currency         string    `json:"currency"`
	Balance          float64   `json:"balance"`
	ConvertedBalance float64   `json:"converted_balance"`
}

type ExchangeRate struct {
	Currency string  `json:"currency"`
	Rate     float64 `json:"rate"`
}

func main() {
	csvFilePath := "/home/javoxir/Downloads/MOCK_DATA.csv"

	_, err := readCSV(csvFilePath)
	if err != nil {
		log.Fatal(err)
	}

	db, err := sql.Open("postgres", "user=javohir dbname=json password=12345 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// err = insertUsersIntoTable(db, users)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	err = updateBalances(db)
	if err != nil {
		log.Fatal(err)
	}

	err = convertToUZS(db)
	if err != nil {
		log.Fatal(err)
	}

}

func printUsersInfo(db *sql.DB) {
	rows, err := db.Query(`
        SELECT id, first_name, last_name, email, currency, balance, converted_balance
        FROM users
    `)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println("Complete information of users:")
	for rows.Next() {
		var user User
		err := rows.Scan(&user.ID, &user.FirstName, &user.LastName, &user.Email, &user.Currency, &user.Balance, &user.ConvertedBalance)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("ID: %s, Name: %s %s, Email: %s, Currency: %s, Balance: %.2f, Converted Balance: %.2f\n",
			user.ID, user.FirstName, user.LastName, user.Email, user.Currency, user.Balance, user.ConvertedBalance)
	}
}
func readCSV(filePath string) ([]User, error) {
    file, err := os.Open(filePath)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    reader := csv.NewReader(file)
    var users []User

    for {
        record, err := reader.Read()
        if err == io.EOF {
            break
        } else if err != nil {
            return nil, err
        }

        balance, err := strconv.ParseFloat(record[5], 64)
        if err != nil {
            log.Printf("Error parsing balance for record %v: %v", record, err)
            continue }

        user := User{
            ID:        uuid.New(),
            FirstName: record[1],
            LastName:  record[2],
            Email:     record[3],
            Currency:  record[4],
            Balance:   balance,
        }
        users = append(users, user)
    }

    return users, nil
}


// func insertUsersIntoTable(db *sql.DB, users []User) error {
// 	for _, user := range users {
// 		_, err := db.Exec(`
// 			INSERT INTO users (
// 				id, first_name, last_name, email, currency, balance
// 			) VALUES ($1, $2, $3, $4, $5, $6)`,
// 			user.ID, user.FirstName, user.LastName, user.Email, user.Currency, user.Balance)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	fmt.Println("Successfully inserted users")
// 	return nil
// }

func updateBalances(db *sql.DB) error {
	rates, err := getExchangeRates(db)
	if err != nil {
		return err
	}

	for _, rate := range rates {
		_, err := db.Exec(`
			UPDATE users
			SET converted_balance = balance * $1
			WHERE currency = $2`,
			rate.Rate, rate.Currency)
		if err != nil {
			return err
		}
	}

	fmt.Println("Successfully updated user balances based on exchange rates")
	return nil
}

func convertToUZS(db *sql.DB) error {
	_, err := getExchangeRates(db)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		UPDATE users
		SET converted_balance = balance * (
			CASE
				WHEN currency = 'UZS' THEN 1
				ELSE (
					SELECT rate
					FROM json
					WHERE ccy = users.currency
					LIMIT 1
				)
			END
		)`)
	if err != nil {
		return err
	}

	fmt.Println("Successfully converted user balances to UZS")
	return nil
}

func getExchangeRates(db *sql.DB) ([]ExchangeRate, error) {
	rows, err := db.Query("SELECT ccy, rate FROM json")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rates []ExchangeRate

	for rows.Next() {
		var rate ExchangeRate
		err := rows.Scan(&rate.Currency, &rate.Rate)
		if err != nil {
			return nil, err
		}
		rates = append(rates, rate)
	}

	return rates, nil
}
