# Create your models here.
from django.db import models
# Create your models here.

class Article(models.Model):
    title = models.CharField(max_length=200)
    content = models.TextField()