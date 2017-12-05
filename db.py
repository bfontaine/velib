# -*- coding: UTF-8 -*-

import json
import gzip

from peewee import SqliteDatabase, CharField, TextField, BooleanField
from peewee import ForeignKeyField, IntegerField, DateTimeField
from peewee import Model as _Model
from arrow.parser import DateTimeParser

#db = SqliteDatabase("velibs.db")
db = SqliteDatabase("data/saved_velibs.db")
datetime_parser = DateTimeParser("en")

class Model(_Model):
    class Meta:
        database = db


class Station(Model):
    recordid = CharField(unique=True, index=True)
    name = CharField()
    json_attrs = TextField()

    def to_json_dict(self):
        d = json.loads(self.json_attrs)
        d["_id"] = self.id
        return d


class Disponibility(Model):
    station = ForeignKeyField(Station, related_name="disponibilites")
    date = DateTimeField()
    status = CharField()
    banking = BooleanField()
    available_bike_stands = IntegerField()
    available_bikes = IntegerField()

    def to_json_dict(self):
        return {
            "_id": self.id,
            "station_id": self.station_id,
            "status": self.status,
            "last_update": self.date,
            "banking": self.banking,
            "available_bike_stands": self.available_bike_stands,
            "available_bikes": self.available_bikes,
        }


def init_db():
    db.create_tables([Station, Disponibility], True)

def save_disponibility(d):
    recordid = d["recordid"]
    fields = d["fields"]

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

    dispo_fields = dict(
        status=fields.pop("status"),
        date=datetime_parser.parse_iso(fields.pop("last_update")),
        banking = fields.pop("banking"),
        available_bike_stands=fields.pop("available_bike_stands"),
        available_bikes=fields.pop("available_bikes"),
        station_id=s.id,
    )

    return Disponibility.create(**dispo_fields)


def save_disponibilities(ds, batch=False):
    if batch:
        return batch_save_disponibilities(ds)

    for d in ds:
        save_disponibility(d)


def batch_save_disponibilities(ds):
    print("Loading stations...")
    # recordid -> station
    stations = {s.recordid: s for s in Station.select()}
    total = 0

    while True:
        batch = []
        n = 0

        for d in ds:
            recordid = d["recordid"]
            fields = d["fields"]

            station = stations[recordid]

            dispo_fields = dict(
                status=fields.pop("status"),
                date=datetime_parser.parse_iso(fields.pop("last_update")),
                banking = fields.pop("banking"),
                available_bike_stands=fields.pop("available_bike_stands"),
                available_bikes=fields.pop("available_bikes"),
                # for some reason it doesn't work with station_id
                station=station,
            )

            batch.append(dispo_fields)
            n += 1

            if n >= 990:
                break

        if not batch:
            break  # end

        print(total)
        total += len(batch)

        with db.atomic():
            Disponibility.insert_many(batch).execute()


def export_stations(filename):
    with gzip.open(filename, "wt", encoding="utf-8") as f:
        for s in Station.select():
            json.dump(s.to_json_dict(), f)
            f.write("\n")

def export_disponibilities(offset, limit, filename):
    print("Exporting from %d w/ limit %d in %s" % (offset, limit, filename))

    n = 0
    step = int(limit/100)
    percentage = 0

    q = Disponibility.select()
    if offset > 0:
        q = q.offset(offset)
    if limit > 0:
        q = q.limit(limit)

    with gzip.open(filename, "wt", encoding="utf-8") as f:
        for dispo in q:
            # try to rebuild in the original format
            d = dispo.to_json_dict()

            json.dump(d, f)
            f.write("\n")

            n += 1
            if n >= step:
                percentage += 1
                print("%d%%" % percentage)
                n = 0
