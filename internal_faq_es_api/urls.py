from django.urls import path
from .views import fetch_results

urlpatterns= [
    path('',fetch_results),
    ]
