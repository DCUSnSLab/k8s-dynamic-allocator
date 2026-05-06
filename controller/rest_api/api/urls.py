from django.urls import path
from . import views

urlpatterns = [
    path('execute/', views.execute_command, name='execute_command'),
    path('health/', views.health_check, name='health_check'),
    path('queue/status/', views.queue_status, name='queue_status'),
    path('ticket/<str:ticket_id>/', views.ticket_detail, name='ticket_detail'),
    path('ticket/<str:ticket_id>/cancel/', views.cancel_ticket, name='cancel_ticket'),
    path('pool/status/', views.pool_status, name='pool_status'),
    path('pool/initialize/', views.initialize_pool, name='initialize_pool'),
    path('compute/release/', views.release_compute_pod, name='release_compute_pod'),
    path('pool/check-stale/', views.check_stale, name='check_stale'),
]
