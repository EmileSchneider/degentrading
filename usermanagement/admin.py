from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from .models import DegenUser

admin.site.register(DegenUser, UserAdmin)
