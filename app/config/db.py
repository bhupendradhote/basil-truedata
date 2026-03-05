import pymysql

def get_db_connection():
    try:
        print("--> Attempting to open connection to localhost with PyMySQL...")
        
        # PyMySQL connection setup
        connection = pymysql.connect(
            host='localhost',
            user='root',
            password='',
            # database='your_database_name' # Uncomment when you create the DB in XAMPP
        )
        
        print("--> Connection attempt finished!")
        print("✅ Successfully connected to MySQL Server!")
        return connection

    except Exception as e:
        print(f"❌ Error caught: {e}")
        return None

if __name__ == "__main__":
    print("🔄 Pinging XAMPP MySQL server...")
    db = get_db_connection()
    
    if db:
        db.close()
        print("🔌 Connection closed.")