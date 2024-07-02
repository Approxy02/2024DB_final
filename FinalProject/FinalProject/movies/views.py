from django.shortcuts import render
from django.db import connection
from django.core.paginator import Paginator


def home(request):
    title = request.GET.get('title')
    director = request.GET.get('director')
    year_from = request.GET.get('year_from')
    year_to = request.GET.get('year_to')

    sql_query = """
    SELECT movie.m_id, movie.title, movie.eng_title, movie.year, movie.country, movie.m_type, 
           movie.status, movie.company, movie.enter_date, director.name as director_name, genre.g_type
    FROM movie
    LEFT JOIN moviedirector ON movie.m_id = moviedirector.m_id
    LEFT JOIN director ON moviedirector.d_id = director.d_id
    LEFT JOIN genre ON movie.m_id = genre.m_id
    WHERE 1=1
    """
    params = []

    if title:
        sql_query += " AND movie.title LIKE %s"
        params.append(f"%{title}%")
    if director:
        sql_query += " AND director.name LIKE %s"
        params.append(f"%{director}%")
    if year_from:
        sql_query += " AND movie.year >= %s"
        params.append(year_from)
    if year_to:
        sql_query += " AND movie.year <= %s"
        params.append(year_to)

    # 디버깅 정보를 출력합니다
    print("SQL Query:", sql_query)
    print("Parameters:", params)

    with connection.cursor() as cursor:
        cursor.execute(sql_query, params)
        rows = cursor.fetchall()

    # Convert rows to a list of dictionaries with genres and directors aggregated
    movies_dict = {}
    for row in rows:
        movie_id = row[0]
        if movie_id not in movies_dict:
            movies_dict[movie_id] = {
                'm_id': row[0],
                'title': row[1],
                'eng_title': row[2],
                'year': row[3],
                'country': row[4],
                'm_type': row[5],
                'status': row[6],
                'company': row[7],
                'enter_date': row[8],
                'directors': [],
                'genres': []
            }
        if row[9] and row[9] not in movies_dict[movie_id]['directors']:
            movies_dict[movie_id]['directors'].append(row[9])
        if row[10] and row[10] not in movies_dict[movie_id]['genres']:
            movies_dict[movie_id]['genres'].append(row[10])

    movies = list(movies_dict.values())

    year_range = range(1874, 2025)

    paginator = Paginator(movies, 20)

    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    return render(request, 'index.html', {'page_obj': page_obj, 'year_range': year_range})
