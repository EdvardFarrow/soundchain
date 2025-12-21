import os
import django
from django.core.asgi import get_asgi_application
from fastapi.staticfiles import StaticFiles

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'soundchain.settings')
django.setup()
django_app = get_asgi_application()

from soundchain.api.main import app as fastapi_app
from django.conf import settings

app = fastapi_app

# Mount Django Static Files
app.mount("/static", StaticFiles(directory=settings.STATIC_ROOT), name="static")

# Mount Django App 
app.mount("/django", django_app)