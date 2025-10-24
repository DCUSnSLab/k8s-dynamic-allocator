from django.urls import path
from . import views

urlpatterns = [
    path('execute/', views.execute_command, name='execute_command'),
    path('health/', views.health_check, name='health_check'),
]
