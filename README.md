# Record-Manager
The record manager handles tables with a fixed schema. Clients can insert records, delete records, update records, and scan through the records in a table. A scan is associated with a search condition and only returns records that match the search condition. Each table should be stored in a separate page file and your record manager should access the pages of the file through the buffer manager (https://github.com/dganesh07/Buffer-Manager).
