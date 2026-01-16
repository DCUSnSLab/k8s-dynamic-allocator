from django.urls import path
from . import views

urlpatterns = [
    path('execute/', views.execute_command, name='execute_command'),
    path('health/', views.health_check, name='health_check'),
    path('pool/status/', views.pool_status, name='pool_status'),
    path('pool/initialize/', views.initialize_pool, name='initialize_pool'),
    path('pool/release/', views.release_backend, name='release_backend'),
]
