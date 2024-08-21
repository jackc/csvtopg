# csvtopg

`csvtopg` copies a CSV to a PostgreSQL database.

Why not just use psql and `\copy`?

* `csvtopg` has easier syntax than `\copy`.
* `csvtopg` will automatically create a table for the CSV data.
* `csvtopg` will automatically detect common data types.

## Installation

The Go tool chain must be installed.

```
$ go install github.com/jackc/csvtopg@latest
```

## Configuring Database Connection

`csvtopg` supports the standard `PG*` environment variables. In addition, the `-d` flag can be used to specify a database URL.

## Example usage

```
$ csvtocsv foo.csv
```

This will create a new table foo_csv and copy foo.csv to it.

## Related

See also the sibling project [pgtocsv)](https://github.com/jackc/pgtocsv) which simplifies exporting the result of a query as a CSV.
