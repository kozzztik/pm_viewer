from django.views import generic
from django import http
from django import urls

from sheets_db import configuration
from pm_viewer import models


class Home(generic.TemplateView):
    template_name = "home.html"

    def get_context_data(self, **kwargs):
        teams = {}
        for member in models.TeamMember.objects.filter(
                hire_date__year__gt=2019, hire_date__month__gt=8,
                hire_date__day__gt=14):
            teams.setdefault(member.team, [])
            if member.name or member.email:
                teams[member.team].append(member)
        return {'teams': teams}

    def get(self, request, *args, **kwargs):
        if not configuration.is_db_configured():
            callback_url = \
                f'{request.scheme}://{request.headers["HOST"]}' + \
                urls.reverse('oauth_callback')
            return http.HttpResponseRedirect(
                configuration.get_db_configuration_url(callback_url))
        return super(Home, self).get(request, *args, **kwargs)


class OAuthCallback(generic.View):
    def get(self, request):
        configuration.configure_db(request)
        return http.HttpResponseRedirect(urls.reverse('home'))
