"""Query script for homework questions 5 and 6.

Run after the Flink jobs have written results to PostgreSQL.

To run:
    python src/job/q5_q6_answers.py
"""

import psycopg2


def query_longest_session(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT PULocationID, num_trips, window_start, window_end
            FROM trip_counts_by_pickup_location_session
            ORDER BY num_trips DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if row:
            return {
                'PULocationID': row[0],
                'num_trips': row[1],
                'window_start': row[2],
                'window_end': row[3],
            }
        return None


def query_max_tip_hour(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT window_start, window_end, total_tip
            FROM tip_amount_by_hour
            ORDER BY total_tip DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if row:
            return {
                'window_start': row[0],
                'window_end': row[1],
                'total_tip': row[2],
            }
        return None


def main():
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='postgres',
        user='postgres',
        password='postgres',
    )
    try:
        print("Longest session (question 5):")
        longest = query_longest_session(conn)
        if longest:
            print(f"  PULocationID: {longest['PULocationID']}")
            print(f"  num_trips: {longest['num_trips']}")
            print(f"  window_start: {longest['window_start']}")
            print(f"  window_end: {longest['window_end']}")
        else:
            print("  No results found; ensure the session window job has run and written to the DB.")

        print("\nHour with max total tips (question 6):")
        max_tip = query_max_tip_hour(conn)
        if max_tip:
            print(f"  window_start: {max_tip['window_start']}")
            print(f"  window_end: {max_tip['window_end']}")
            print(f"  total_tip: {max_tip['total_tip']}")
        else:
            print("  No results found; ensure the tumbling window job has run and written to the DB.")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
