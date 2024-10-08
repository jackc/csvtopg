package cmd

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/jackc/csvtopg/csvtopg"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
)

var databaseURL string
var table string
var dropTable bool

var rootCmd = &cobra.Command{
	Use:   "csvtopg CSVFILE",
	Short: "copies a CSV to a PostgreSQL database",
	Long: `copies a CSV to a PostgreSQL database

To read from STDIN use "-" as the CSVFILE argument. This will buffer the entire input in memory.

PG* environment variables such as PGDATABASE can be used to configure the
connection.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()

		csvFilename := args[0]
		var reader io.ReadSeeker
		if csvFilename == "-" {
			buf, err := io.ReadAll(os.Stdin)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to read from STDIN: %v\n", err)
				os.Exit(1)
			}
			reader = bytes.NewReader(buf)
		} else {
			file, err := os.Open(csvFilename)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to open CSV file: %v\n", err)
				os.Exit(1)
			}
			defer file.Close()
			reader = file
		}

		conn, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to connect to database: %v\n", err)
			os.Exit(1)
		}
		defer conn.Close(ctx)

		csvReader := csv.NewReader(reader)
		columns, err := csvtopg.AnalyzeColumns(pgtype.NewConnInfo(), csvReader.Read)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to analyze columns: %v\n", err)
			os.Exit(1)
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to start transaction: %v\n", err)
			os.Exit(1)
		}

		tableName := computeTableName(table, csvFilename)

		if dropTable {
			_, err = tx.Exec(ctx, fmt.Sprintf("drop table if exists %s", tableName))
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to drop existing table: %v\n", err)
				os.Exit(1)
			}
		}

		err = csvtopg.CreateTable(ctx, tx, tableName, columns)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create table: %v\n", err)
			os.Exit(1)
		}

		_, err = reader.Seek(0, io.SeekStart)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to rewind CSV: %v\n", err)
			os.Exit(1)
		}
		csvReader = csv.NewReader(reader)
		_, err = csvtopg.CopyRows(ctx, tx, tableName, columns, csvReader.Read)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to copy rows: %v\n", err)
			os.Exit(1)
		}

		err = tx.Commit(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to commit transaction: %v\n", err)
			os.Exit(1)
		}
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().StringVarP(&databaseURL, "database-url", "d", "", "Database URL or DSN")
	rootCmd.Flags().StringVarP(&table, "table", "t", "", "Table in which to insert data")
	rootCmd.Flags().BoolVar(&dropTable, "drop-table", false, "Drop existing table if it exist")
}

func computeTableName(tablename, filename string) string {
	if tablename != "" {
		return tablename
	}

	if filename == "-" {
		return "stdin"
	}

	return csvtopg.NormalizeIdentifier(filename)
}
