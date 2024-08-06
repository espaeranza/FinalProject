from django.urls import path
from . import views

app_name = 'recommendation'
urlpatterns = [
    path('', views.mainpage, name='mainpage'),
    path('recommendation/', views.index, name='index'),
    path('category/', views.category, name='category'),

]
