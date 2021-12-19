from django import forms


class ConfigureProjectForm(forms.Form):

    sheet_id = forms.CharField(required=True)
    code = forms.CharField(widget=forms.HiddenInput)
    scope = forms.CharField(widget=forms.HiddenInput)
    state = forms.CharField(widget=forms.HiddenInput)
