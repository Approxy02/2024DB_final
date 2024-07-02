from django.db import models


class Movie(models.Model):
    m_id = models.AutoField(primary_key=True)
    title = models.CharField(max_length=500, blank=True, null=True)
    eng_title = models.CharField(max_length=500, blank=True, null=True)
    year = models.IntegerField(blank=True, null=True)
    country = models.CharField(max_length=100, blank=True, null=True)
    m_type = models.CharField(max_length=10, blank=True, null=True)
    status = models.CharField(max_length=30, blank=True, null=True)
    company = models.CharField(max_length=1000, blank=True, null=True)
    enter_date = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = 'movie'


class Director(models.Model):
    d_id = models.AutoField(primary_key=True)
    name = models.CharField(unique=True, max_length=100, blank=True, null=True)

    class Meta:
        db_table = 'director'


class MovieDirector(models.Model):
    m_d_id = models.AutoField(primary_key=True)
    m = models.ForeignKey(Movie, on_delete=models.CASCADE, blank=True, null=True)
    d = models.ForeignKey(Director, on_delete=models.CASCADE, blank=True, null=True)

    class Meta:
        db_table = 'moviedirector'


class Genre(models.Model):
    g_id = models.AutoField(primary_key=True)
    m = models.ForeignKey(Movie, on_delete=models.CASCADE)
    g_type = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        db_table = 'genre'
        unique_together = (('g_id', 'm'),)
