"""
ASGI config for pm_viewer project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/4.0/howto/deployment/asgi/
"""

import os

from django.core.asgi import get_asgi_application

if os.path.exists('settings_local.py'):
    settings_file = 'settings_local'
else:
    settings_file = 'settings'
os.environ.setdefault('DJANGO_SETTINGS_MODULE', settings_file)

application = get_asgi_application()
