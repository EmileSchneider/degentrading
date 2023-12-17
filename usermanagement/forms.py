from django import forms
from django.contrib.auth.forms import UserCreationForm
from .models import DegenUser

class DegenUserCreationForm(UserCreationForm):
    class Meta(UserCreationForm.Meta):
        model = DegenUser
        # Include fields you want in the form, e.g., 'username', 'email', 'first_name', 'last_name'
        fields = UserCreationForm.Meta.fields + ('email',)
