from django.urls import path
from . import views

urlpatterns = [
    path('execute/', views.execute_command, name='execute_command'),
    path('health/', views.health_check, name='health_check'),
    path('ticket/<str:ticket_id>/', views.ticket_detail, name='ticket_detail'),
    path('ticket/<str:ticket_id>/cancel/', views.cancel_ticket, name='cancel_ticket'),
    path('pool/status/', views.pool_status, name='pool_status'),
    path('pool/initialize/', views.initialize_pool, name='initialize_pool'),
    path('pool/release/', views.release_backend, name='release_backend'),
    path('pool/check-stale/', views.check_stale, name='check_stale'),
]
