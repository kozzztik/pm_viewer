from django.db import models


class TeamMember(models.Model):
    team = models.TextField(db_column="Команда")
    name = models.TextField(db_column="Имя")
    email = models.TextField(db_column="Почта", unique=True)
    position = models.TextField(db_column="Позиция")
    grade = models.TextField(db_column="Грейд")
    evaluation = models.TextField(db_column="Оценка")
    mark = models.TextField(db_column="Метка")
    salary = models.IntegerField(db_column="ЗП")
    salary_target = models.IntegerField(db_column='Таргет ЗП')
    hire_date = models.DateField(db_column='Найм')

    class Meta:
        db_table = 'Team'


class eNPSReply(models.Model):
    timestamp = models.DateField(db_column='Отметка времени')
    email = models.ForeignKey(
        TeamMember, on_delete=models.CASCADE,
        db_column="Адрес электронной почты",
        to_field='email', related_name='enps_replies')
    value = models.IntegerField(
        db_column="Насколько ты счастлив/счастлива работать в компании?")

    class Meta:
        db_table = 'Отзывы eNPS'
