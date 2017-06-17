# -*- coding: UTF-8 -*-

import json

from peewee import SqliteDatabase, CharField, TextField, BooleanField
from peewee import ForeignKeyField, IntegerField, DateTimeField
from peewee import Model as _Model
from arrow.parser import DateTimeParser

db = SqliteDatabase("velibs.db")
datetime_parser = DateTimeParser("en")

class Model(_Model):
    class Meta:
        database = db


class Station(Model):
    recordid = CharField(unique=True, index=True)
    name = CharField()
    json_attrs = TextField()


class Disponibility(Model):
    station = ForeignKeyField(Station, related_name="disponibilites")
    date = DateTimeField()
    status = CharField()
    banking = BooleanField()
    available_bike_stands = IntegerField()
    available_bikes = IntegerField()


def init_db():
    db.create_tables([Station, Disponibility], True)


def save_disponibility(d):
    recordid = d["recordid"]
    fields = d["fields"]

    dispo = Disponibility(
        status=fields.pop("status"),
        date=datetime_parser.parse_iso(fields.pop("last_update")),
        banking = fields.pop("banking"),
        available_bike_stands=fields.pop("available_bike_stands"),
        available_bikes=fields.pop("available_bikes"),
    )

    try:
        s = Station.get(recordid=recordid)
    except Station.DoesNotExist:
        name = fields["name"]
        json_attrs = json.dumps(d, sort_keys=True, ensure_ascii=False)
        s = Station.create(
            recordid=recordid,
            name=name,
            json_attrs=json_attrs,
        )

    dispo.station = s
    dispo.save()
    return dispo


def save_disponibilities(ds):
    for d in ds:
        save_disponibility(d)
