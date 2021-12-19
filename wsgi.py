"""
WSGI config for pm_viewer project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/4.0/howto/deployment/wsgi/
"""

import os

from django.core.wsgi import get_wsgi_application

if os.path.exists('settings_local.py'):
    settings_file = 'settings_local'
else:
    settings_file = 'settings'
os.environ.setdefault('DJANGO_SETTINGS_MODULE', settings_file)

application = get_wsgi_application()
