from django.urls import path
from . import views

urlpatterns = [
    path('health/', views.health_check, name='health_check'),
    path('pool/status/', views.pool_status, name='pool_status'),
    path('pool/initialize/', views.initialize_pool, name='initialize_pool'),
    path('pool/release/', views.release_backend, name='release_backend'),
    path('interactive/start/', views.interactive_start, name='interactive_start'),
    path('interactive/end/', views.interactive_end, name='interactive_end'),
]
