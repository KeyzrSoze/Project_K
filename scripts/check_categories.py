from services.db import DatabaseManager

def check_categories():
    """
    Connects to the database, queries category distribution and total markets,
    and prints the results.
    """
    try:
        db = DatabaseManager()
        with db.get_connection() as conn:
            cursor = conn.cursor()

            # --- Query 1: Category Distribution ---
            print("\n--- Category Distribution in 'series_registry' ---")
            cursor.execute("SELECT category, COUNT(*) FROM series_registry GROUP BY category ORDER BY COUNT(*) DESC;")
            results = cursor.fetchall()

            if results:
                print(f"{'Category':<20} | {'Count'}")
                print("-------------------------")
                for row in results:
                    print(f"{row[0]:<20} | {row[1]}")
            else:
                print("No data found in series_registry.")

            # --- Query 2: Total Market Count ---
            print("\n--- Total Market Count ---")
            cursor.execute("SELECT COUNT(*) FROM market_registry;")
            total_markets = cursor.fetchone()
            
            if total_markets:
                print(f"Total markets in 'market_registry': {total_markets[0]}")
            else:
                print("No data found in market_registry.")

    except Exception as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    check_categories()
