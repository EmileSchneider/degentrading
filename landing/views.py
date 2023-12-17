from django.shortcuts import render
from psycopg2 import connect

# Create your views here.
def index(request):
    return render(request, 'landing/index.html')

