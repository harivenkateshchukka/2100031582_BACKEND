import mysql.connector
from mysql.connector import errorcode

# Connection configuration
config = {
    'user': 'root',
    'password': 'Venkatesh@1607',
    'host': '127.0.0.1',
    'database': 'retail_store',
}
# Establish connection
try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    print("Connected to MySQL database")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)

# Create tables
def create_tables(cursor):
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Customers (
        CustomerID INT PRIMARY KEY,
        FirstName VARCHAR(50),
        LastName VARCHAR(50),
        Email VARCHAR(100),
        DateOfBirth DATE
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Products (
        ProductID INT PRIMARY KEY,
        ProductName VARCHAR(100),
        Price DECIMAL(10, 2)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Orders (
        OrderID INT PRIMARY KEY,
        CustomerID INT,
        OrderDate DATE,
        FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS OrderItems (
        OrderItemID INT PRIMARY KEY,
        OrderID INT,
        ProductID INT,
        Quantity INT,
        FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
        FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
    )
    ''')

# Insert sample data
def insert_sample_data(cursor):
    cursor.execute("INSERT INTO Customers VALUES (1, 'John', 'Doe', 'john.doe@example.com', '1985-01-15')")
    cursor.execute("INSERT INTO Customers VALUES (2, 'Jane', 'Smith', 'jane.smith@example.com', '1990-06-20')")

    cursor.execute("INSERT INTO Products VALUES (1, 'Laptop', 1000)")
    cursor.execute("INSERT INTO Products VALUES (2, 'Smartphone', 600)")
    cursor.execute("INSERT INTO Products VALUES (3, 'Headphones', 100)")

    cursor.execute("INSERT INTO Orders VALUES (1, 1, '2023-01-10')")
    cursor.execute("INSERT INTO Orders VALUES (2, 2, '2023-01-12')")

    cursor.execute("INSERT INTO OrderItems VALUES (1, 1, 1, 1)")
    cursor.execute("INSERT INTO OrderItems VALUES (2, 1, 3, 2)")
    cursor.execute("INSERT INTO OrderItems VALUES (3, 2, 2, 1)")
    cursor.execute("INSERT INTO OrderItems VALUES (4, 2, 3, 1)")

    conn.commit()

# Queries

# 1. List all customers
def list_all_customers(cursor):
    cursor.execute("SELECT * FROM Customers")
    return cursor.fetchall()

# 2. Find all orders placed in January 2023
def find_orders_in_january_2023(cursor):
    cursor.execute("SELECT * FROM Orders WHERE MONTH(OrderDate) = 1 AND YEAR(OrderDate) = 2023")
    return cursor.fetchall()

# 3. Get the details of each order, including the customer name and email
def get_order_details(cursor):
    cursor.execute('''
    SELECT Orders.OrderID, Customers.FirstName, Customers.LastName, Customers.Email, Orders.OrderDate
    FROM Orders
    JOIN Customers ON Orders.CustomerID = Customers.CustomerID
    ''')
    return cursor.fetchall()

# 4. List the products purchased in a specific order (e.g., OrderID = 1)
def list_products_in_order(cursor, order_id):
    cursor.execute('''
    SELECT Products.ProductName, OrderItems.Quantity
    FROM OrderItems
    JOIN Products ON OrderItems.ProductID = Products.ProductID
    WHERE OrderItems.OrderID = %s
    ''', (order_id,))
    return cursor.fetchall()

# 5. Calculate the total amount spent by each customer
def calculate_total_spent_by_customers(cursor):
    cursor.execute('''
    SELECT Customers.CustomerID, Customers.FirstName, Customers.LastName, SUM(Products.Price * OrderItems.Quantity) as TotalSpent
    FROM Customers
    JOIN Orders ON Customers.CustomerID = Orders.CustomerID
    JOIN OrderItems ON Orders.OrderID = OrderItems.OrderID
    JOIN Products ON OrderItems.ProductID = Products.ProductID
    GROUP BY Customers.CustomerID
    ''')
    return cursor.fetchall()

# 6. Find the most popular product (the one that has been ordered the most)
def find_most_popular_product(cursor):
    cursor.execute('''
    SELECT Products.ProductName, SUM(OrderItems.Quantity) as TotalQuantity
    FROM Products
    JOIN OrderItems ON Products.ProductID = OrderItems.ProductID
    GROUP BY Products.ProductID
    ORDER BY TotalQuantity DESC
    LIMIT 1
    ''')
    return cursor.fetchone()

# 7. Get the total number of orders and the total sales amount for each month in 2023
def get_monthly_sales_2023(cursor):
    cursor.execute('''
    SELECT DATE_FORMAT(Orders.OrderDate, '%Y-%m') as Month, COUNT(Orders.OrderID) as TotalOrders, SUM(Products.Price * OrderItems.Quantity) as TotalSales
    FROM Orders
    JOIN OrderItems ON Orders.OrderID = OrderItems.OrderID
    JOIN Products ON OrderItems.ProductID = Products.ProductID
    WHERE YEAR(Orders.OrderDate) = 2023
    GROUP BY Month
    ''')
    return cursor.fetchall()

# 8. Find customers who have spent more than $1000
def find_high_spending_customers(cursor):
    cursor.execute('''
    SELECT Customers.CustomerID, Customers.FirstName, Customers.LastName, SUM(Products.Price * OrderItems.Quantity) as TotalSpent
    FROM Customers
    JOIN Orders ON Customers.CustomerID = Orders.CustomerID
    JOIN OrderItems ON Orders.OrderID = OrderItems.OrderID
    JOIN Products ON OrderItems.ProductID = Products.ProductID
    GROUP BY Customers.CustomerID
    HAVING TotalSpent > 1000
    ''')
    return cursor.fetchall()

# Create tables and insert sample data
create_tables(cursor)
insert_sample_data(cursor)

# Testing the functions
print("All customers:")
print(list_all_customers(cursor))

print("\nOrders in January 2023:")
print(find_orders_in_january_2023(cursor))

print("\nOrder details:")
print(get_order_details(cursor))

print("\nProducts in order 1:")
print(list_products_in_order(cursor, 1))

print("\nTotal amount spent by each customer:")
print(calculate_total_spent_by_customers(cursor))

print("\nMost popular product:")
print(find_most_popular_product(cursor))

print("\nMonthly sales in 2023:")
print(get_monthly_sales_2023(cursor))

print("\nCustomers who have spent more than $1000:")
print(find_high_spending_customers(cursor))

# Close the connection
cursor.close()
conn.close()
