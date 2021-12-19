from django.views import generic
from django.conf import settings
from django import http
from django.core import exceptions

from django import urls

from pm_viewer import forms
from sheets_db import configuration
from pm_viewer import models


class Home(generic.TemplateView):
    template_name = "home.html"

    def get_context_data(self, **kwargs):
        teams = {}
        for member in models.TeamMember.objects.all():
            teams.setdefault(member.team, [])
            if member.name or member.email:
                teams[member.team].append(member)
        return {'teams': teams}

    def get(self, request, *args, **kwargs):
        if not settings.PROJECT_CONFIGURED:
            url, state = configuration.flow.authorization_url(
                access_type='offline')
            return http.HttpResponseRedirect(url)
        return super(Home, self).get(request, *args, **kwargs)


class OAuthCallback(generic.FormView):
    template_name = "configure_project.html"
    form_class = forms.ConfigureProjectForm

    def get_initial(self):
        return {
            'code': self.request.GET['code'],
            'scope': self.request.GET['scope'],
            'state': self.request.GET['state'],
        }

    def form_valid(self, form):
        if form.data['scope'] != configuration.GOOGLE_SCOPES[0]:
            raise exceptions.PermissionDenied('Google sheets access denied')
        configuration.initial_configure_db(
            form.data['code'], form.data['sheet_id'])
        return http.HttpResponseRedirect(urls.reverse('home'))
