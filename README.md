# SimpleSqlRs
Rust library that provides SQL like operation (select, join, group by..) over in memory tables. This library is particulary usefull for complex manipulations of csv files. A csv file can be loaded in memory as a table using Table::load_tsv from the table module. This is not a database and it's not concurrent, you can imagine this as an SQL with select queries only.
