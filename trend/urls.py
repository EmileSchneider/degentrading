from django.contrib.auth import views as auth_views
from django.urls import path

from .views import btc_momentum

urlpatterns = [
    path('momentum', btc_momentum, name='momentum')
]
