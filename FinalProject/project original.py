import pandas as pd
import numpy as np
from db_conn import open_db, close_db
import sys


def read_excel_into_mysql():
    excel_file = "movie_list.xls"

    conn, cur = open_db()

    # Read the first and second sheets
    df1 = pd.read_excel(excel_file, skiprows=4, sheet_name=0)
    df2 = pd.read_excel(excel_file, sheet_name=1, header=None)
    df2.columns = df1.columns

    movie_table = "university.movie"
    director_table = "university.director"
    movie_director_table = "university.moviedirector"
    genre_table = "university.genre"

    create_sql = f"""
        DROP TABLE IF EXISTS {genre_table};
        DROP TABLE IF EXISTS {movie_director_table};
        DROP TABLE IF EXISTS {movie_table};
        DROP TABLE IF EXISTS {director_table};

        CREATE TABLE {movie_table} (
            m_id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(500),
            eng_title VARCHAR(500),
            year INT,
            country VARCHAR(100),
            m_type VARCHAR(10),
            status VARCHAR(30),
            company VARCHAR(1000),
            enter_date DATETIME DEFAULT NOW()
        );

        CREATE TABLE {director_table} (
            d_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            UNIQUE(name)
        );

        CREATE TABLE {movie_director_table} (
            m_id INT,
            d_id INT,
            m_d_id INT AUTO_INCREMENT PRIMARY KEY,
            FOREIGN KEY (m_id) REFERENCES {movie_table}(m_id),
            FOREIGN KEY (d_id) REFERENCES {director_table}(d_id)
        );

        CREATE TABLE {genre_table} (
            g_id INT AUTO_INCREMENT,
            m_id INT,
            g_type VARCHAR(100),
            PRIMARY KEY (g_id, m_id),
            FOREIGN KEY (m_id) REFERENCES {movie_table}(m_id)
        );
    """

    cur.execute(create_sql)
    conn.commit()

    insert_movie_sql = f"""
        INSERT INTO {movie_table} 
        (title, eng_title, year, country, m_type, status, company) 
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    insert_director_sql = f"""
        INSERT IGNORE INTO {director_table} (name)
        VALUES (%s);
    """
    select_director_sql = f"""
        SELECT d_id 
        FROM {director_table} 
        WHERE name=%s;
    """
    insert_movie_director_sql = f"""
        INSERT INTO {movie_director_table} (m_id, d_id)
        VALUES (%s, %s);
    """
    insert_genre_sql = f"""
        INSERT INTO {genre_table} (m_id, g_type)
        VALUES (%s, %s);
    """

    def process_dataframe(df):
        for i, r in df.iterrows():
            # Convert row to tuple and replace NaN with None
            row = tuple(None if pd.isna(x) else x for x in r)

            movie_data = (row[0], row[1], row[2], row[3], row[4], row[6], row[8])
            try:
                cur.execute(insert_movie_sql, movie_data)
                movie_id = cur.lastrowid

                # Insert directors
                directors = row[7].split(',') if row[7] else []
                director_data = []
                for director in directors:
                    director_name = director.strip()
                    cur.execute(insert_director_sql, (director_name,))
                    director_id = cur.lastrowid
                    cur.execute(select_director_sql, (director_name,))
                    result = cur.fetchone()

                    director_id = result['d_id']
                    director_data.append((movie_id, director_id))

                if director_data:
                    cur.executemany(insert_movie_director_sql, director_data)

                # Insert genres
                genres = row[5].split(',') if row[5] else []
                genre_data = [(movie_id, genre.strip()) for genre in genres]

                if genre_data:
                    cur.executemany(insert_genre_sql, genre_data)

                if (i + 1) % 1000 == 0:
                    print(f"{i + 1} rows processed")

            except Exception as e:
                print(f"Error processing row {i + 1}: {e}")
                print(row)

    # def process_dataframe(df):
    #     for i, r in df.iterrows():
    #         # Convert row to tuple and replace NaN with None
    #         row = tuple(None if pd.isna(x) else x for x in r)
    #
    #         movie_data = (row[0], row[1], row[2], row[3], row[4], row[6], row[8])
    #         try:
    #             cur.execute(insert_movie_sql, movie_data)
    #             movie_id = cur.lastrowid
    #
    #             # Insert directors
    #             directors = row[7].split(',') if row[7] else []
    #             for director in directors:
    #                 director_name = director.strip()
    #                 cur.execute(insert_director_sql, (director_name,))
    #                 director_id = cur.lastrowid
    #                 cur.execute(select_director_sql, (director_name,))
    #                 result = cur.fetchone()
    #
    #                 director_id = result['d_id']
    #                 cur.execute(insert_movie_director_sql, (movie_id, director_id))
    #
    #             # Insert genres
    #             genres = row[5].split(',') if row[5] else []
    #             for genre in genres:
    #                 cur.execute(insert_genre_sql, (movie_id, genre.strip()))
    #
    #             if (i + 1) % 1000 == 0:
    #                 print(f"{i + 1} rows processed")
    #
    #         except Exception as e:
    #             print(f"Error processing row {i + 1}: {e}")
    #             print(row)

    process_dataframe(df1)
    process_dataframe(df2)

    conn.commit()
    close_db(conn, cur)


def make_index():
    conn, cur = open_db()

    create_index_sql = """
        CREATE INDEX idx_movie_title ON university.movie (title);
        CREATE INDEX idx_movie_year ON university.movie (year);
        CREATE INDEX idx_director_name ON university.director (name);
    """

    cur.execute(create_index_sql)
    print("Indexes created")
    conn.commit()
    close_db(conn, cur)



if __name__ == '__main__':
    read_excel_into_mysql()
    make_index()
