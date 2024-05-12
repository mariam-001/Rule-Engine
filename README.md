Rule Engine:
Rule Engine is a Scala application designed to read data from a CSV file, apply discount calculations based on various criteria,
and store the results in an Oracle database table. The discounts are calculated based on factors such as remaining days until expiry, product type, purchase channel, payment method, etc.

Features:
CSV Data Parsing: The application parses data from a CSV file containing order information.
Discount Calculation: Discounts are calculated based on various criteria such as remaining days until expiry, product type, purchase channel, payment method, etc.
Oracle Database Integration: Results are stored in an Oracle database table after discount calculations.
Logging: Log messages are written to a log file (logs.txt) to track application execution.


Prerequisites:
Before running the application, ensure you have the following installed:
Scala
Java Development Kit (JDK)
Oracle Database (with appropriate privileges to create and insert into tables)
Oracle JDBC Driver (included in the project dependencies)
