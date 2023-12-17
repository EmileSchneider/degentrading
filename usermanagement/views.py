from django.urls import reverse_lazy
from django.views import generic
from .forms import DegenUserCreationForm


class SignUpView(generic.CreateView):
    form_class = DegenUserCreationForm
    success_url = reverse_lazy('login')  # Redirect to login page after signup
    template_name = 'registration/signup.html'
