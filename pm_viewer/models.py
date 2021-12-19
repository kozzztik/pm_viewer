from django.db import models


class TeamMember(models.Model):
    team = models.TextField(db_column="Команда")
    name = models.TextField(db_column="Имя")
    email = models.TextField(db_column="Почта")
    position = models.TextField(db_column="Позиция")
    grade = models.TextField(db_column="Грейд")
    evaluation = models.TextField(db_column="Оценка")
    mark = models.TextField(db_column="Метка")

    class Meta:
        db_table = 'Team'
