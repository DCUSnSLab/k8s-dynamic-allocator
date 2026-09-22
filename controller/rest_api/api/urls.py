from django.urls import path
from . import views

urlpatterns = [
    path('execute/', views.execute_command, name='execute_command'),
    path('health/', views.health_check, name='health_check'),
    path('queue/status/', views.queue_status, name='queue_status'),
    path('ticket/<str:ticket_id>/', views.ticket_detail, name='ticket_detail'),
    path('ticket/<str:ticket_id>/cancel/', views.cancel_ticket, name='cancel_ticket'),
    path('compute/status/', views.buffer_status, name='buffer_status'),
    path('compute/initialize/', views.initialize_buffer, name='initialize_buffer'),
    path('compute/release/', views.release_compute_pod, name='release_compute_pod'),
]
