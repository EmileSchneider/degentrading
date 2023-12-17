import psycopg2
from django.http import JsonResponse
from django.shortcuts import render

from .service.momentum import get_momentum_ranking


# Create your views here.
def btc_momentum(request):
    ranking = get_momentum_ranking()
    ranking.sort(key=lambda x: x[1])

    context = {
        'ranking': [i[0] for i in ranking],
        'active_tab': 'momentum'
    }

    return render(request, 'trend/momentum.html', context)
